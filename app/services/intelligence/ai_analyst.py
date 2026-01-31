"""
AI Analyst Module - Preserved
"""
import json
import random
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import List
import requests
import pytz
import yfinance as yf
import pandas as pd
from groq import Groq

from app.core.config import settings
from app.models.domain import MarketRegime

import logging
logger = logging.getLogger("VOLGUARD")

class NewsScout:
    def __init__(self):
        self.rss_url = "https://news.google.com/rss/search?q=stock+market+india+business+sensex+nifty&hl=en-IN&gl=IN&ceid=IN:en"

    def get_headlines(self, limit=5):
        try:
            response = requests.get(self.rss_url, timeout=5)
            if response.status_code != 200: 
                return []
            root = ET.fromstring(response.content)
            headlines = []
            for item in root.findall('./channel/item')[:limit]:
                title = item.find('title').text
                if "-" in title: 
                    title = title.rpartition('-')[0].strip()
                headlines.append(title)
            return headlines
        except Exception:
            return []

class EventRadar:
    def __init__(self):
        self.url = "https://economic-calendar.tradingview.com/events"
        self.headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://in.tradingview.com"}

    def get_todays_events(self):
        try:
            now_utc = datetime.now(pytz.utc)
            payload = {
                "from": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": (now_utc + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "countries": "IN,US",
                "minImportance": "1"
            }
            r = requests.get(self.url, params=payload, headers=self.headers, timeout=5)
            events = []
            for e in r.json().get('result', []):
                if e.get('importance', 0) > 0:
                    dt_utc = datetime.strptime(e['date'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                    dt_ist = dt_utc.astimezone(settings.IST)
                    events.append(f"[{e['country']}] {e['title']} @ {dt_ist.strftime('%I:%M %p')} IST")
            return events[:7]
        except:
            return []

def get_market_metrics():
    tickers = {"^NSEI": "Nifty 50", "^INDIAVIX": "India VIX", "ES=F": "US Futures", "GC=F": "Gold"}
    try:
        data = yf.download(list(tickers.keys()), period="5d", progress=False)['Close']
        metrics = {}
        for symbol, name in tickers.items():
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if symbol in data.columns.levels[1]: 
                        s = data.xs(symbol, axis=1, level=1).dropna()
                    else: 
                        s = pd.Series()
                else: 
                    s = data[symbol].dropna()
                if s.empty and symbol in data: 
                    s = data[symbol].dropna()
                
                if len(s) >= 2:
                    metrics[name] = f"{((s.iloc[-1] - s.iloc[-2]) / s.iloc[-2]) * 100:+.2f}%"
                else: 
                    metrics[name] = "0.00%"
            except: 
                metrics[name] = "Err"
        return metrics
    except: 
        return {}

class IndiaAIAnalyst:
    def __init__(self, api_key):
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"
        self.scout = NewsScout()
        self.radar = EventRadar()

    def get_morning_brief(self) -> MarketRegime:
        news = self.scout.get_headlines()
        events = self.radar.get_todays_events()
        data = get_market_metrics()

        system = "You are a Nifty 50 Risk Officer. Output JSON only."
        prompt = f"""
        Analyze Indian Market: {datetime.now(settings.IST).strftime('%d %b %Y')}.
        DATA: {json.dumps(data)}
        EVENTS: {json.dumps(events)}
        NEWS: {json.dumps(news)}
        RISK RULES:
        - War/Terror/Sanctions + Gold UP = FEAR (Risk 8-10).
        - VIX > +5% = VOLATILITY (Risk 6-7).
        - Fed/RBI/CPI/Election = EVENT_RISK.
        OUTPUT JSON:
        {{
            "risk_score": (int 1-10),
            "nifty_view": "GAP_UP"|"GAP_DOWN"|"FLAT",
            "strategy": "IRON_CONDOR"|"CREDIT_SPREAD"|"CASH_ONLY",
            "reason": "Brief summary."
        }}
        """
        try:
            chat = self.client.chat.completions.create(
                messages=[{"role":"system","content":system}, {"role":"user","content":prompt}],
                model=self.model,
                response_format={"type": "json_object"}
            )
            res = json.loads(chat.choices[0].message.content)
            return MarketRegime(
                risk_score=res.get('risk_score', 5),
                nifty_view=res.get('nifty_view', 'FLAT'),
                strategy=res.get('strategy', 'IRON_CONDOR'),
                reasoning=res.get('reason', 'AI Analysis')
            )
        except Exception as e:
            return MarketRegime(5, "FLAT", "IRON_CONDOR", f"AI Error: {str(e)}")
