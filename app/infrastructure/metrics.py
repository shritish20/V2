"""
Prometheus metrics - Preserved exactly from original
"""
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# Global registry
PROM_REGISTRY = CollectorRegistry()

# System metrics
sys_uptime = Gauge("volguard_uptime_seconds", "Seconds since VolGuard started", registry=PROM_REGISTRY)
trade_counter = Counter("volguard_trades_total", "Trades opened by strategy and expiry", ["strategy", "expiry_type"], registry=PROM_REGISTRY)
trade_pnl = Gauge("volguard_trade_pnl_inr", "Real-time P&L per trade_id", ["trade_id", "strategy"], registry=PROM_REGISTRY)
order_exec_latency = Histogram("volguard_order_latency_seconds", "Time between place_order and final fill/cancel", ["side", "role"], buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0), registry=PROM_REGISTRY)
order_fill_ratio = Gauge("volguard_order_fill_ratio", "Filled qty / requested qty", ["instrument_key", "side", "role"], registry=PROM_REGISTRY)
slippage_pct = Histogram("volguard_slippage_percent", "abs(actual_price – expected_price)/expected_price * 100", ["instrument_key", "side"], buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.5), registry=PROM_REGISTRY)
order_timeout_counter = Counter("volguard_order_timeouts_total", "Orders that timed out", ["side", "role"], registry=PROM_REGISTRY)
greeks_delta = Gauge("volguard_portfolio_delta", "Net portfolio delta", registry=PROM_REGISTRY)
greeks_theta = Gauge("volguard_portfolio_theta", "Net portfolio theta", registry=PROM_REGISTRY)
greeks_gamma = Gauge("volguard_portfolio_gamma", "Net portfolio gamma", registry=PROM_REGISTRY)
greeks_vega = Gauge("volguard_portfolio_vega", "Net portfolio vega", registry=PROM_REGISTRY)
margin_used_gauge = Gauge("volguard_margin_used_percent", "Used margin / available margin * 100", registry=PROM_REGISTRY)
iv_rank_weekly = Gauge("volguard_iv_rank_weekly", "Weekly IV percentile vs 1-year", registry=PROM_REGISTRY)
iv_rank_monthly = Gauge("volguard_iv_rank_monthly", "Monthly IV percentile vs 1-year", registry=PROM_REGISTRY)
vov_zscore = Gauge("volguard_vov_zscore", "Vol-of-Vol z-score", registry=PROM_REGISTRY)
price_staleness_sec = Gauge("volguard_price_staleness_seconds", "Seconds since last LTP update", registry=PROM_REGISTRY)
circuit_breaker_active = Gauge("volguard_circuit_breaker_active", "1 = breaker open (no new trades)", registry=PROM_REGISTRY)
db_queue_size = Gauge("volguard_db_queue_size", "Outstanding writes in DB writer queue", registry=PROM_REGISTRY)

def record_trade_open(strategy: str, expiry_type: str, trade_id: str):
    trade_counter.labels(strategy=strategy, expiry_type=expiry_type).inc()

def record_order_fill(leg: dict, elapsed: float):
    role, side, key = leg.get("role", "UNKNOWN").upper(), leg["side"].upper(), leg["key"]
    req_qty, filled = leg["qty"], leg.get("filled_qty", 0)
    ratio = filled / req_qty if req_qty else 0
    order_fill_ratio.labels(instrument_key=key, side=side, role=role).set(ratio)
    order_exec_latency.labels(side=side, role=role).observe(elapsed)

def record_slippage(leg: dict):
    entry, expected = leg.get("entry_price", 0.0), leg.get("ltp", entry)
    if expected > 0:
        pct = abs(entry - expected) / expected * 100
        slippage_pct.labels(instrument_key=leg["key"], side=leg["side"].upper()).observe(pct)

def update_greeks(delta, theta, gamma, vega):
    greeks_delta.set(delta)
    greeks_theta.set(theta)
    greeks_gamma.set(gamma)
    greeks_vega.set(vega)

def update_margin_pct(used: float, avail: float):
    margin_used_gauge.set(used / avail * 100 if avail else 0)

def start_metrics_server(port=8000):
    from prometheus_client import start_http_server, REGISTRY
    REGISTRY = PROM_REGISTRY
    import logging
    logger = logging.getLogger("VOLGUARD")
    logger.info(f"📊 Prometheus metrics served on 0.0.0.0:{port}/metrics")
    start_http_server(port, registry=PROM_REGISTRY)
