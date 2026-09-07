"""
QubHub Telemetry Server
يستقبل ويحلل إحصائيات الاستخدام (مجهولة الهوية)
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import sqlite3
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

# إعداد Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler('telemetry.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('telemetry')

# إعداد FastAPI
app = FastAPI(
    title="QubHub Telemetry Server",
    version="1.0.0",
    description="Privacy-first telemetry for QubHub Agent"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# إعداد قاعدة البيانات
DB_PATH = Path("./data/events.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def init_db():
    """تهيئة الجداول والفهارس"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                event TEXT NOT NULL,
                data TEXT,
                timestamp DATETIME NOT NULL,
                version TEXT,
                platform TEXT,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_user_time 
            ON events(user_id, timestamp);
            
            CREATE INDEX IF NOT EXISTS idx_event_time 
            ON events(event, timestamp);
            
            CREATE INDEX IF NOT EXISTS idx_received 
            ON events(received_at);
        """)
    logger.info("✅ Database initialized at %s", DB_PATH)


init_db()


# ═══════════════════════════════════════════════════════════
# Pydantic Models
# ═══════════════════════════════════════════════════════════

class TelemetryEvent(BaseModel):
    user_id: str = Field(..., min_length=8, max_length=64)
    event: str = Field(..., min_length=1, max_length=64)
    data: Optional[Dict[str, Any]] = {}
    timestamp: str
    version: str
    platform: str


class BulkEvents(BaseModel):
    events: List[TelemetryEvent]


# ═══════════════════════════════════════════════════════════
# Endpoints: Receive Data
# ═══════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return {
        "service": "QubHub Telemetry",
        "version": "1.0.0",
        "status": "operational",
        "docs": "/docs"
    }


@app.post("/v1/events")
async def receive_event(event: TelemetryEvent, request: Request):
    """استقبال حدث واحد"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """INSERT INTO events 
                   (user_id, event, data, timestamp, version, platform) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    event.user_id,
                    event.event,
                    json.dumps(event.data, ensure_ascii=False),
                    event.timestamp,
                    event.version,
                    event.platform,
                )
            )
        logger.info("📥 Event: %s from user %s", event.event, event.user_id[:8])
        return {"status": "ok", "received": 1}
    except Exception as e:
        logger.error("❌ Error storing event: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/v1/events/bulk")
async def receive_bulk(bulk: BulkEvents, request: Request):
    """استقبال عدة أحداث دفعة واحدة"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(
                """INSERT INTO events 
                   (user_id, event, data, timestamp, version, platform) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        e.user_id,
                        e.event,
                        json.dumps(e.data, ensure_ascii=False),
                        e.timestamp,
                        e.version,
                        e.platform,
                    )
                    for e in bulk.events
                ]
            )
        logger.info("📥 Bulk: %d events", len(bulk.events))
        return {"status": "ok", "received": len(bulk.events)}
    except Exception as e:
        logger.error("❌ Bulk error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════
# Endpoints: Analytics
# ═══════════════════════════════════════════════════════════

@app.get("/stats/summary")
async def get_summary():
    """ملخص عام"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        
        users = conn.execute(
            "SELECT COUNT(DISTINCT user_id) as c FROM events"
        ).fetchone()['c']
        
        events = conn.execute(
            "SELECT COUNT(*) as c FROM events"
        ).fetchone()['c']
        
        active_24h = conn.execute("""
            SELECT COUNT(DISTINCT user_id) as c 
            FROM events 
            WHERE received_at >= datetime('now', '-24 hours')
        """).fetchone()['c']
        
        active_7d = conn.execute("""
            SELECT COUNT(DISTINCT user_id) as c 
            FROM events 
            WHERE received_at >= datetime('now', '-7 days')
        """).fetchone()['c']
        
        by_type = conn.execute("""
            SELECT event, COUNT(*) as count 
            FROM events 
            GROUP BY event 
            ORDER BY count DESC
            LIMIT 20
        """).fetchall()
        
        by_platform = conn.execute("""
            SELECT platform, COUNT(*) as count 
            FROM events 
            GROUP BY platform
        """).fetchall()
        
        return {
            "total_users": users,
            "total_events": events,
            "active_24h": active_24h,
            "active_7d": active_7d,
            "events_by_type": [dict(r) for r in by_type],
            "events_by_platform": [dict(r) for r in by_platform],
        }


@app.get("/stats/daily")
async def get_daily(days: int = 14):
    """إحصائيات يومية"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT 
                DATE(received_at) as date,
                COUNT(DISTINCT user_id) as users,
                COUNT(*) as events
            FROM events
            WHERE received_at >= datetime('now', ? || ' days')
            GROUP BY DATE(received_at)
            ORDER BY date DESC
        """, (-days,))
        return [dict(r) for r in cursor.fetchall()]


@app.get("/stats/features")
async def get_features():
    """استخدام الميزات"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT 
                json_extract(data, '$.feature') as feature,
                COUNT(*) as count
            FROM events
            WHERE event = 'feature_used'
              AND json_extract(data, '$.feature') IS NOT NULL
            GROUP BY feature
            ORDER BY count DESC
        """)
        return [dict(r) for r in cursor.fetchall()]


@app.get("/stats/models")
async def get_models():
    """استخدام النماذج"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT 
                json_extract(data, '$.model') as model,
                COUNT(*) as count
            FROM events
            WHERE event = 'model_switched'
              AND json_extract(data, '$.model') IS NOT NULL
            GROUP BY model
            ORDER BY count DESC
        """)
        return [dict(r) for r in cursor.fetchall()]


@app.get("/stats/users/{user_id}")
async def get_user(user_id: str):
    """سجل مستخدم معين (للدعم الفني)"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT event, data, timestamp, platform, version
            FROM events
            WHERE user_id = ?
            ORDER BY timestamp DESC
            LIMIT 100
        """, (user_id,))
        rows = [dict(r) for r in cursor.fetchall()]
        for row in rows:
            if row['data']:
                row['data'] = json.loads(row['data'])
        return {"user_id": user_id, "events": rows}


@app.get("/health")
async def health():
    """فحص الصحة"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("SELECT 1")
        return {"status": "healthy", "db": "ok"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting Telemetry Server on http://localhost:8000")
    logger.info("📖 API docs: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
