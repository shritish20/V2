"""
Database Writer - Preserved exactly but wrapped for FastAPI lifespan
"""
import os
import sqlite3
import json
import queue
import threading
import logging
from datetime import date, datetime
from typing import Optional, Dict, List

logger = logging.getLogger("VOLGUARD")

class DatabaseWriter:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.message_queue = queue.Queue(maxsize=10000)
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True, name="DB-Writer")
        self.write_error_count = 0
        self.max_write_errors = 10
        self._init_schema()
        self.thread.start()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-64000;")
        conn.commit()
        return conn

    def _init_schema(self):
        conn = self._get_connection()
        schema = """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            strategy_type TEXT,
            expiry_date DATE,
            entry_premium REAL,
            max_risk REAL,
            status TEXT,
            legs_json TEXT,
            exit_reason TEXT,
            final_pnl REAL
        );
        CREATE TABLE IF NOT EXISTS positions (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            instrument_key TEXT,
            strike REAL,
            option_type TEXT,
            side TEXT,
            qty INTEGER,
            entry_price REAL,
            current_price REAL,
            delta REAL,
            status TEXT,
            FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
        );
        CREATE TABLE IF NOT EXISTS risk_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,
            severity TEXT,
            description TEXT,
            action_taken TEXT
        );
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS order_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            order_id TEXT,
            instrument_key TEXT,
            side TEXT,
            qty INTEGER,
            price REAL,
            status TEXT,
            filled_qty INTEGER,
            avg_price REAL,
            message TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            paper_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            instrument_key TEXT,
            side TEXT,
            qty INTEGER,
            entry_price REAL,
            exit_price REAL,
            pnl REAL,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS performance_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            total_trades INTEGER,
            winning_trades INTEGER,
            losing_trades INTEGER,
            total_pnl REAL,
            peak_capital REAL,
            current_capital REAL,
            drawdown_pct REAL,
            sharpe_ratio REAL
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE,
            trades_executed INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            largest_win REAL DEFAULT 0,
            largest_loss REAL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_risk_events_timestamp ON risk_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_order_log_timestamp ON order_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_paper_trades_timestamp ON paper_trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);
        """
        try:
            conn.executescript(schema)
            conn.commit()
            logger.info("Database schema initialized")
        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            raise
        finally:
            conn.close()

    def _worker(self):
        conn = self._get_connection()
        logger.info("DB Writer thread started")
        while self.running:
            try:
                msg = self.message_queue.get(timeout=1)
                if msg['type'] == 'execute':
                    conn.execute(msg['sql'], msg['params'])
                    conn.commit()
                    self.write_error_count = 0
                elif msg['type'] == 'executescript':
                    conn.executescript(msg['sql'])
                    conn.commit()
                    self.write_error_count = 0
                elif msg['type'] == 'shutdown':
                    break
            except queue.Empty:
                continue
            except sqlite3.Error as e:
                logger.error(f"DB Write error: {e}")
                self.write_error_count += 1
                conn.rollback()
                if self.write_error_count >= self.max_write_errors:
                    logger.critical(f"DB Writer exceeded {self.max_write_errors} errors. Attempting reconnect.")
                    try:
                        conn.close()
                        conn = self._get_connection()
                        self.write_error_count = 0
                        logger.info("DB reconnection successful")
                    except Exception as reconn_err:
                        logger.critical(f"DB reconnection failed: {reconn_err}")
            except Exception as e:
                logger.error(f"DB Worker unexpected error: {e}")
                conn.rollback()
        conn.close()
        logger.info("DB Writer thread stopped")

    def execute(self, sql: str, params: tuple = (), timeout: float = 5.0):
        try:
            self.message_queue.put({'type': 'execute', 'sql': sql, 'params': params}, timeout=timeout)
        except queue.Full:
            logger.error(f"DB queue full! Dropping write: {sql[:100]}")

    def executescript(self, sql: str, timeout: float = 5.0):
        try:
            self.message_queue.put({'type': 'executescript', 'sql': sql}, timeout=timeout)
        except queue.Full:
            logger.error("DB queue full! Dropping script execution")

    def save_trade(self, trade_id: str, strategy: str, expiry: date, legs: List[Dict], entry_premium: float, max_risk: float):
        self.execute(
            "INSERT INTO trades (trade_id, strategy_type, expiry_date, entry_premium, max_risk, status, legs_json) VALUES (?, ?, ?, ?, ?, 'OPEN', ?)",
            (trade_id, strategy, expiry, entry_premium, max_risk, json.dumps(legs))
        )

    def update_trade_exit(self, trade_id: str, exit_reason: str, final_pnl: float):
        self.execute(
            "UPDATE trades SET status='CLOSED', exit_reason=?, final_pnl=? WHERE trade_id=?",
            (exit_reason, final_pnl, trade_id)
        )

    def log_risk_event(self, event_type: str, severity: str, desc: str, action: str):
        self.execute(
            "INSERT INTO risk_events (event_type, severity, description, action_taken) VALUES (?, ?, ?, ?)",
            (event_type, severity, desc, action)
        )

    def log_order(self, order_id: str, instrument_key: str, side: str, qty: int, price: float, status: str, filled_qty: int = 0, avg_price: float = 0.0, message: str = ""):
        self.execute(
            "INSERT INTO order_log (order_id, instrument_key, side, qty, price, status, filled_qty, avg_price, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (order_id, instrument_key, side, qty, price, status, filled_qty, avg_price, message)
        )

    def set_state(self, key: str, value: str):
        self.execute(
            "INSERT OR REPLACE INTO system_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (key, value)
        )

    def get_state(self, key: str) -> Optional[str]:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM system_state WHERE key = ?", (key,))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"State read error: {e}")
            return None

    def update_system_vitals(self, latency_ms: float, cpu_usage: float, ram_usage: float):
        vitals = {
            "latency": round(latency_ms, 2),
            "cpu": round(cpu_usage, 1),
            "ram": round(ram_usage, 1),
            "queue_size": self.message_queue.qsize(),
            "updated_at": datetime.now().strftime("%H:%M:%S")
        }
        self.set_state("system_vitals", json.dumps(vitals))

    def log_paper_trade(self, trade_id: str, instrument_key: str, side: str, qty: int, entry_price: float, exit_price: float = 0, pnl: float = 0, status: str = "OPEN"):
        self.execute(
            "INSERT INTO paper_trades (trade_id, instrument_key, side, qty, entry_price, exit_price, pnl, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (trade_id, instrument_key, side, qty, entry_price, exit_price, pnl, status)
        )

    def update_daily_stats(self, trades: int = 0, pnl: float = 0, largest_win: float = 0, largest_loss: float = 0):
        today = date.today()
        self.execute(
            "INSERT INTO daily_stats (date, trades_executed, total_pnl, largest_win, largest_loss) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(date) DO UPDATE SET trades_executed=trades_executed+?, total_pnl=total_pnl+?, "
            "largest_win=MAX(largest_win, ?), largest_loss=MIN(largest_loss, ?)",
            (today, trades, pnl, largest_win, largest_loss, trades, pnl, largest_win, largest_loss)
        )

    def get_daily_stats(self, target_date: date = None) -> Optional[Dict]:
        if not target_date:
            target_date = date.today()
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM daily_stats WHERE date = ?", (target_date,))
            row = cursor.fetchone()
            conn.close()
            if row:
                return {
                    'trades_executed': row['trades_executed'],
                    'total_pnl': row['total_pnl'],
                    'largest_win': row['largest_win'],
                    'largest_loss': row['largest_loss']
                }
            return None
        except Exception as e:
            logger.error(f"Daily stats read error: {e}")
            return None

    def export_trade_journal(self, output_path: str):
        try:
            import pandas as pd
            conn = self._get_connection()
            trades_df = pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC", conn)
            risk_events_df = pd.read_sql_query("SELECT * FROM risk_events ORDER BY timestamp DESC", conn)
            # Check dry run mode from state
            dry_run = self.get_state("dry_run_mode")
            if dry_run and dry_run == "True":
                paper_df = pd.read_sql_query("SELECT * FROM paper_trades ORDER BY timestamp DESC", conn)
                paper_df.to_csv(os.path.join(output_path, "paper_trades.csv"), index=False)
            conn.close()
            trades_df.to_csv(os.path.join(output_path, "trades.csv"), index=False)
            risk_events_df.to_csv(os.path.join(output_path, "risk_events.csv"), index=False)
            logger.info(f"Trade journal exported to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False

    def shutdown(self):
        logger.info("Shutting down DB Writer...")
        self.running = False
        try:
            self.message_queue.put({'type': 'shutdown'}, timeout=2)
        except queue.Full:
            pass
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            logger.warning("DB Writer thread did not exit cleanly")
