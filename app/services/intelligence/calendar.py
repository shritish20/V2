"""
Calendar Engine - Preserved
"""
import requests
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from app.models.domain import EconomicEvent
from app.core.config import settings

logger = logging.getLogger("VOLGUARD")

class CalendarEngine:
    @staticmethod
    def fetch_calendar(days_ahead: int = 7) -> List[EconomicEvent]:
        try:
            from_timestamp = int(datetime.now().timestamp())
            to_timestamp = int((datetime.now() + timedelta(days=days_ahead)).timestamp())
            
            params = {
                'from': from_timestamp,
                'to': to_timestamp,
                'countries': 'IN,US',
                'importance': '1,2,3'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                settings.ECONOMIC_CALENDAR_URL,
                params=params,
                headers=headers,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.warning(f"Calendar API returned {response.status_code}")
                return []
            
            data = response.json()
            events = []
            
            for item in data.get('result', []):
                event_title = item.get('title', '').strip()
                country = item.get('country', '')
                event_timestamp = item.get('date', 0)
                importance = item.get('importance', 0)
                forecast = item.get('forecast', 'N/A')
                previous = item.get('previous', 'N/A')
                
                if event_timestamp == 0:
                    continue
                
                event_date = datetime.fromtimestamp(event_timestamp, tz=settings.IST)
                now = datetime.now(settings.IST)
                hours_until = (event_date - now).total_seconds() / 3600
                days_until = int(hours_until / 24)
                
                is_veto = any(keyword in event_title for keyword in settings.VETO_KEYWORDS)
                
                if is_veto:
                    event_type = "VETO"
                    impact_level = "CRITICAL"
                elif any(keyword in event_title for keyword in settings.HIGH_IMPACT_KEYWORDS):
                    event_type = "HIGH_IMPACT"
                    impact_level = "HIGH"
                elif any(keyword in event_title for keyword in settings.MEDIUM_IMPACT_KEYWORDS):
                    event_type = "MEDIUM_IMPACT"
                    impact_level = "MEDIUM"
                else:
                    event_type = "LOW_IMPACT"
                    impact_level = "LOW"
                
                square_off_time = None
                if is_veto and hours_until > 0:
                    if hours_until <= 24:
                        square_off_time = event_date - timedelta(hours=2)
                    elif hours_until <= 48:
                        day_before = event_date.date() - timedelta(days=1)
                        square_off_time = datetime.combine(
                            day_before, 
                            settings.SQUARE_OFF_TIME_IST
                        ).replace(tzinfo=settings.IST)
                
                events.append(EconomicEvent(
                    title=event_title,
                    country=country,
                    event_date=event_date,
                    impact_level=impact_level,
                    event_type=event_type,
                    forecast=str(forecast),
                    previous=str(previous),
                    days_until=days_until,
                    hours_until=hours_until,
                    is_veto_event=is_veto,
                    suggested_square_off_time=square_off_time
                ))
            
            events.sort(key=lambda x: x.event_date)
            logger.info(f"📅 Fetched {len(events)} events ({sum(1 for e in events if e.is_veto_event)} veto)")
            return events
            
        except Exception as e:
            logger.error(f"Calendar fetch failed: {e}")
            return []

    @staticmethod
    def analyze_veto_risk(events: List[EconomicEvent], current_time: datetime) -> Tuple[bool, Optional[str], bool, Optional[float]]:
        veto_events = [e for e in events if e.is_veto_event and e.hours_until > 0]
        
        if not veto_events:
            return False, None, False, None
        
        nearest = min(veto_events, key=lambda x: x.hours_until)
        square_off_needed = False
        if nearest.hours_until <= 48:
            square_off_needed = True
        
        return True, nearest.title, square_off_needed, nearest.hours_until

    @staticmethod
    def calculate_event_impact(events: List[EconomicEvent]) -> Tuple[
        List[EconomicEvent], List[EconomicEvent], bool, Optional[datetime]
    ]:
        veto_events = [e for e in events if e.is_veto_event and e.hours_until > 0]
        high_impact = [e for e in events if e.event_type == "HIGH_IMPACT" and e.hours_until > 0]
        
        square_off_needed = False
        square_off_time = None
        
        if veto_events:
            nearest = min(veto_events, key=lambda x: x.hours_until)
            if nearest.hours_until <= 48:
                square_off_needed = True
                square_off_time = nearest.suggested_square_off_time
        
        return veto_events, high_impact, square_off_needed, square_off_time
