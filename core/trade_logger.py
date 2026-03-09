import sqlite3, logging, threading
from datetime import datetime, timedelta
log = logging.getLogger("bot.db")

class TradeLogger:
    def __init__(self, db_path="trades.db"):
        self.db_path = db_path
        self._db_lock = threading.Lock()
        self._init_db()
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                platform TEXT NOT NULL, exchange TEXT NOT NULL, contract_id TEXT,
                amount_sats INTEGER, buy_price REAL, sell_price REAL,
                currency TEXT DEFAULT 'EUR', premium_pct REAL,
                exchange_fee REAL DEFAULT 0, platform_fee REAL DEFAULT 0,
                network_fee REAL DEFAULT 0, net_profit REAL DEFAULT 0,
                payment_method TEXT, status TEXT DEFAULT 'completed',
                withdrawal_fee REAL DEFAULT 0, funding_fee REAL DEFAULT 0,
                spot_at_buy REAL DEFAULT 0, spot_at_sell REAL DEFAULT 0)""")
            # Migrate: add columns if missing (existing DBs)
            for col, default in [("withdrawal_fee", 0), ("funding_fee", 0),
                                 ("spot_at_buy", 0), ("spot_at_sell", 0)]:
                try:
                    conn.execute(f"ALTER TABLE trades ADD COLUMN {col} REAL DEFAULT {default}")
                except Exception:
                    pass  # Column already exists
            conn.execute("""CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                platform TEXT, event_type TEXT NOT NULL, data TEXT)""")
            conn.commit()
        self._deduplicate()
    def _deduplicate(self):
        with sqlite3.connect(self.db_path) as conn:
            # Remove duplicates with same contract_id
            conn.execute("""DELETE FROM trades WHERE id NOT IN (
                SELECT MIN(id) FROM trades GROUP BY contract_id
            ) AND contract_id != '' AND contract_id IS NOT NULL""")
            # Remove duplicates with empty contract_id but same platform+amount+day
            conn.execute("""DELETE FROM trades WHERE (contract_id = '' OR contract_id IS NULL)
                AND id NOT IN (
                    SELECT MIN(id) FROM trades
                    WHERE contract_id = '' OR contract_id IS NULL
                    GROUP BY platform, amount_sats, date(timestamp)
                )""")
            conn.commit()
            removed = conn.execute("SELECT changes()").fetchone()[0]
            if removed: log.info(f"Deduplicated {removed} duplicate trades")
    def log_trade(self, **kw):
        contract_id = kw.get("contract_id", "")
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            if contract_id:
                existing = conn.execute("SELECT id FROM trades WHERE contract_id=?", (contract_id,)).fetchone()
                if existing:
                    log.debug(f"Trade {contract_id} already logged, skipping duplicate")
                    return
            conn.execute("""INSERT INTO trades (timestamp,platform,exchange,contract_id,
                amount_sats,buy_price,sell_price,currency,premium_pct,exchange_fee,
                platform_fee,network_fee,net_profit,payment_method,status,
                withdrawal_fee,funding_fee,spot_at_buy,spot_at_sell)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                kw.get("timestamp", datetime.now().isoformat()),
                kw.get("platform",""), kw.get("exchange",""), kw.get("contract_id",""),
                kw.get("amount_sats",0), kw.get("buy_price",0), kw.get("sell_price",0),
                kw.get("currency","EUR"), kw.get("premium_pct",0), kw.get("exchange_fee",0),
                kw.get("platform_fee",0), kw.get("network_fee",0), kw.get("net_profit",0),
                kw.get("payment_method",""), kw.get("status","completed"),
                kw.get("withdrawal_fee",0), kw.get("funding_fee",0),
                kw.get("spot_at_buy",0), kw.get("spot_at_sell",0)))
            conn.commit()
    def log_event(self, platform, event_type, data=""):
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO events (timestamp,platform,event_type,data) VALUES (?,?,?,?)",
                (datetime.now().isoformat(), platform, event_type, data))
            conn.commit()
    def get_recent(self, limit=10):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
    def get_daily_summary(self, platform=None):
        today = datetime.now().strftime("%Y-%m-%d")
        where = "WHERE timestamp LIKE ? AND status='completed'"
        params = [f"{today}%"]
        if platform: where += " AND platform=?"; params.append(platform)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(f"SELECT COUNT(*),COALESCE(SUM(amount_sats),0),COALESCE(SUM(sell_price),0),COALESCE(SUM(net_profit),0),COALESCE(AVG(premium_pct),0) FROM trades {where}", params).fetchone()
            return {"count":row[0],"total_sats":row[1],"total_revenue":row[2],"total_profit":row[3],"avg_premium":row[4]}
    def get_period_summary(self, days=30, platform=None):
        since = (datetime.now() - timedelta(days=days)).isoformat()
        where = "WHERE timestamp >= ? AND status='completed'"
        params = [since]
        if platform: where += " AND platform=?"; params.append(platform)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(f"SELECT COUNT(*),COALESCE(SUM(amount_sats),0),COALESCE(SUM(sell_price),0),COALESCE(SUM(net_profit),0),COALESCE(AVG(premium_pct),0) FROM trades {where}", params).fetchone()
            return {"count":row[0],"total_sats":row[1],"total_revenue":row[2],"total_profit":row[3],"avg_premium":row[4],"days":days}
    def get_platform_breakdown(self, days=30):
        since = (datetime.now() - timedelta(days=days)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT platform,COUNT(*),COALESCE(SUM(net_profit),0),COALESCE(AVG(premium_pct),0) FROM trades WHERE timestamp >= ? AND status='completed' GROUP BY platform", (since,)).fetchall()
            return [{"platform":r[0],"count":r[1],"profit":r[2],"avg_premium":r[3]} for r in rows]
