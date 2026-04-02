"""
╔══════════════════════════════════════════════════════════╗
║     🤖 Telegram Multi-Account Manager v2.0              ║
║     Advanced Userbot + Control Bot System               ║
║     By: @YourUsername                                   ║
╚══════════════════════════════════════════════════════════╝

المتطلبات:
pip install telethon pyTelegramBotAPI apscheduler aiosqlite cryptography colorlog pytz python-dotenv
"""

import os, json, asyncio, logging, random, signal, sys, time
import threading, base64, re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ══════════════════════════════════════════════
#  التحقق من المكتبات
# ══════════════════════════════════════════════
_missing = []
for _pkg in ["aiosqlite","telebot","telethon","apscheduler","cryptography","pytz"]:
    try: __import__(_pkg)
    except ImportError: _missing.append(_pkg)

if _missing:
    print(f"❌ مكتبات ناقصة: {', '.join(_missing)}")
    print(f"🔧 ثبّتها: pip install {' '.join(_missing)}")
    sys.exit(1)

import aiosqlite
import telebot
from telebot.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove,
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    PhoneCodeInvalidError, AuthKeyError,
    UserBannedInChannelError, PeerFloodError,
    ChannelPrivateError, InviteHashInvalidError,
    ChatWriteForbiddenError,
)
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import pytz

# ══════════════════════════════════════════════
#  🎨 الألوان والتنسيق
# ══════════════════════════════════════════════

class Style:
    """تنسيق موحد لجميع رسائل البوت"""

    # ── Headers ──
    HEADER = "━" * 30
    DIVIDER = "┄" * 25
    LINE = "─" * 20

    # ── Icons ──
    SUCCESS   = "✅"
    ERROR     = "❌"
    WARNING   = "⚠️"
    INFO      = "ℹ️"
    LOADING   = "⏳"
    DONE      = "🎯"
    FIRE      = "🔥"
    STAR      = "⭐"
    LOCK      = "🔐"
    PHONE     = "📱"
    BOT       = "🤖"
    ACCOUNT   = "👤"
    TASK      = "⚙️"
    SCHEDULE  = "📅"
    STATS     = "📊"
    LOG       = "📋"
    SEND      = "📤"
    RECEIVE   = "📥"
    GROUP     = "👥"
    SHIELD    = "🛡"
    CLOCK     = "🕐"
    ROCKET    = "🚀"
    GEAR      = "⚙️"
    DATABASE  = "🗄"
    ONLINE    = "🟢"
    OFFLINE   = "🔴"
    IDLE      = "🟡"
    BANNED    = "🚫"
    CROWN     = "👑"
    LIGHTNING = "⚡"
    CHART     = "📈"
    BELL      = "🔔"
    PIN       = "📌"
    FOLDER    = "📁"
    KEY       = "🔑"
    LINK      = "🔗"
    SEARCH    = "🔍"
    TRASH     = "🗑"
    EDIT      = "✏️"
    BACK      = "◀️"
    NEXT      = "▶️"
    UP        = "⬆️"
    DOWN      = "⬇️"
    REFRESH   = "🔄"
    PLAY      = "▶️"
    STOP      = "⏹"
    PAUSE     = "⏸"
    FORWARD   = "↪️"
    NEW       = "🆕"
    ID        = "🆔"
    BROADCAST = "📡"
    WORLD     = "🌍"
    COPY      = "📋"

    @staticmethod
    def header(title: str, icon: str = "🤖") -> str:
        return (
            f"{icon} *{title}*\n"
            f"`{'━' * 28}`"
        )

    @staticmethod
    def section(title: str, icon: str = "•") -> str:
        return f"\n{icon} *{title}*"

    @staticmethod
    def field(label: str, value: Any, icon: str = "▸") -> str:
        return f"{icon} {label}: `{value}`"

    @staticmethod
    def success(msg: str) -> str:
        return f"✅ *{msg}*"

    @staticmethod
    def error(msg: str) -> str:
        return f"❌ *خطأ:* {msg}"

    @staticmethod
    def warning(msg: str) -> str:
        return f"⚠️ *تحذير:* {msg}"

    @staticmethod
    def loading(msg: str) -> str:
        return f"⏳ _{msg}..._"

    @staticmethod
    def code(text: str) -> str:
        return f"`{text}`"

    @staticmethod
    def bold(text: str) -> str:
        return f"*{text}*"

    @staticmethod
    def italic(text: str) -> str:
        return f"_{text}_"

    @staticmethod
    def status_badge(is_connected: bool, is_banned: bool = False) -> str:
        if is_banned:   return "🚫 محظور"
        if is_connected: return "🟢 متصل"
        return "🔴 منفصل"

    @staticmethod
    def progress_bar(current: int, total: int, length: int = 10) -> str:
        if total == 0: return "░" * length
        filled = int(length * current / total)
        return "█" * filled + "░" * (length - filled)

    @staticmethod
    def format_number(n: int) -> str:
        if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
        if n >= 1_000:     return f"{n/1_000:.1f}K"
        return str(n)

    @staticmethod
    def format_time(dt_str: str) -> str:
        try:
            dt = datetime.fromisoformat(dt_str)
            now = datetime.utcnow()
            diff = now - dt
            total = int(diff.total_seconds())
            if total < 60:    return "منذ لحظات"
            if total < 3600:  return f"منذ {total//60} دقيقة"
            if total < 86400: return f"منذ {total//3600} ساعة"
            if diff.days == 1:       return "أمس"
            return dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return dt_str[:16] if dt_str else "—"

    @staticmethod
    def task_type_icon(task_type: str) -> str:
        return {
            "send_message": "💬",
            "join_group":   "👥",
            "leave_group":  "🚪",
            "forward":      "↪️",
        }.get(task_type, "⚙️")

    @staticmethod
    def task_type_ar(task_type: str) -> str:
        return {
            "send_message": "إرسال رسالة",
            "join_group":   "انضمام لجروب",
            "leave_group":  "مغادرة جروب",
            "forward":      "إعادة توجيه",
        }.get(task_type, task_type)

    @staticmethod
    def trigger_type_ar(ttype: str) -> str:
        return {
            "once":     "مرة واحدة",
            "interval": "تكرار",
            "cron":     "Cron",
        }.get(ttype, ttype)


S = Style()

# ══════════════════════════════════════════════
#  ⚙️ الإعدادات
# ══════════════════════════════════════════════

BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
ADMIN_IDS   = [int(x) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip()]
ENC_KEY     = os.getenv("ENCRYPTION_KEY", "")
TIMEZONE    = os.getenv("TIMEZONE", "Africa/Cairo")
# دعم Session Strings متعددة
def load_all_sessions():
    """جلب كل Session Strings من متغيرات البيئة"""
    sessions = {}
    for key, value in os.environ.items():
        if key.startswith("SESSION_"):
            session_num = key.replace("SESSION_", "")
            sessions[session_num] = value
    return sessions

SESSION_ACCOUNTS = load_all_sessions()
DEFAULT_API_ID = int(os.getenv("DEFAULT_API_ID", "2040"))
DEFAULT_API_HASH = os.getenv("DEFAULT_API_HASH", "b18441a1ff607e10a989891a5462e627")
LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO")
MIN_DELAY   = float(os.getenv("MIN_DELAY", "3.0"))
MAX_DELAY   = float(os.getenv("MAX_DELAY", "8.0"))
JOIN_DELAY  = float(os.getenv("JOIN_DELAY", "30.0"))
RATE_MSGS   = int(os.getenv("RATE_LIMIT_MESSAGES", "20"))
RATE_PERIOD = int(os.getenv("RATE_LIMIT_PERIOD", "60"))
BOT_NAME    = os.getenv("BOT_NAME", "TG Manager")
VERSION     = "2.0.0"

BASE_DIR     = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / "sessions"
DATA_DIR     = BASE_DIR / "data"
LOGS_DIR     = BASE_DIR / "logs"
DB_PATH      = DATA_DIR / "manager.db"

for _d in [SESSIONS_DIR, DATA_DIR, LOGS_DIR]:
    _d.mkdir(exist_ok=True)

TZ = pytz.timezone(TIMEZONE)

# ══════════════════════════════════════════════
#  📝 Logging
# ══════════════════════════════════════════════

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOGS_DIR / "system.log"), encoding="utf-8"),
    ]
)
for _noisy in ["telethon","apscheduler","urllib3","httpx"]:
    logging.getLogger(_noisy).setLevel(logging.WARNING)
logger = logging.getLogger("TGManager")

# ══════════════════════════════════════════════
#  🔐 التشفير
# ══════════════════════════════════════════════

class EncryptionManager:
    def __init__(self):
        self._f = self._init()

    def _init(self) -> Fernet:
        if not ENC_KEY:
            logger.warning("⚠️ ENCRYPTION_KEY غير موجود - مفتاح مؤقت")
            return Fernet(Fernet.generate_key())
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32,
            salt=b"tgmanager_v2_salt", iterations=480000,
        )
        return Fernet(base64.urlsafe_b64encode(kdf.derive(ENC_KEY.encode())))

    def enc(self, t: str) -> str:
        return self._f.encrypt(t.encode()).decode() if t else ""

    def dec(self, t: str) -> str:
        try:
            return self._f.decrypt(t.encode()).decode() if t else ""
        except Exception:
            return t

    @staticmethod
    def gen_key() -> str:
        return Fernet.generate_key().decode()

_enc = EncryptionManager()

# ══════════════════════════════════════════════
#  🗄 قاعدة البيانات
# ══════════════════════════════════════════════

_db: Optional[aiosqlite.Connection] = None

async def db_connect():
    global _db
    _db = await aiosqlite.connect(str(DB_PATH))
    _db.row_factory = aiosqlite.Row
    await _db.execute("PRAGMA journal_mode=WAL;")
    await _db.execute("PRAGMA foreign_keys=ON;")
    await _db.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            phone        TEXT UNIQUE NOT NULL,
            api_id       TEXT NOT NULL,
            api_hash     TEXT NOT NULL,
            session_name TEXT UNIQUE NOT NULL,
            username     TEXT,
            full_name    TEXT,
            is_active    INTEGER DEFAULT 1,
            is_banned    INTEGER DEFAULT 0,
            msg_count    INTEGER DEFAULT 0,
            join_count   INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now')),
            last_used    TEXT,
            notes        TEXT
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            account_id  INTEGER NOT NULL,
            task_type   TEXT NOT NULL,
            target      TEXT NOT NULL,
            content     TEXT,
            parse_mode  TEXT DEFAULT 'markdown',
            is_active   INTEGER DEFAULT 1,
            run_count   INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now')),
            created_by  INTEGER,
            FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS schedules (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id      INTEGER NOT NULL,
            trigger_type TEXT NOT NULL,
            trigger_data TEXT NOT NULL,
            last_run     TEXT,
            next_run     TEXT,
            run_count    INTEGER DEFAULT 0,
            max_runs     INTEGER DEFAULT -1,
            is_active    INTEGER DEFAULT 1,
            created_at   TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS execution_logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id     INTEGER,
            schedule_id INTEGER,
            account_id  INTEGER,
            status      TEXT NOT NULL,
            message     TEXT,
            duration_ms INTEGER DEFAULT 0,
            executed_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS notifications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            type       TEXT NOT NULL,
            title      TEXT NOT NULL,
            body       TEXT,
            is_read    INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS blacklist (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            target     TEXT UNIQUE NOT NULL,
            reason     TEXT,
            added_at   TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_acc  ON tasks(account_id);
        CREATE INDEX IF NOT EXISTS idx_logs_task  ON execution_logs(task_id);
        CREATE INDEX IF NOT EXISTS idx_logs_acc   ON execution_logs(account_id);
        CREATE INDEX IF NOT EXISTS idx_logs_date  ON execution_logs(executed_at);
    """)
    await _db.commit()
    logger.info("✅ قاعدة البيانات جاهزة")

async def db_close():
    if _db: await _db.close()

# ─── Accounts ───

async def db_add_account(phone, api_id, api_hash, session_name, username=None, full_name=None, notes=None) -> int:
    await _db.execute(
        "INSERT INTO accounts (phone,api_id,api_hash,session_name,username,full_name,notes) VALUES (?,?,?,?,?,?,?)",
        (phone, _enc.enc(str(api_id)), _enc.enc(api_hash), session_name, username, full_name, notes)
    )
    await _db.commit()
    cur = await _db.execute("SELECT last_insert_rowid()")
    return (await cur.fetchone())[0]

def _decrypt_acc(d: dict) -> dict:
    d["api_id"]   = _enc.dec(d["api_id"])
    d["api_hash"] = _enc.dec(d["api_hash"])
    return d

async def db_get_account(aid: int) -> Optional[dict]:
    cur = await _db.execute("SELECT * FROM accounts WHERE id=?", (aid,))
    row = await cur.fetchone()
    return _decrypt_acc(dict(row)) if row else None

async def db_get_all_accounts(active_only=True) -> List[dict]:
    q = "SELECT * FROM accounts" + (" WHERE is_active=1 AND is_banned=0" if active_only else "")
    q += " ORDER BY id"
    cur = await _db.execute(q)
    return [_decrypt_acc(dict(r)) for r in await cur.fetchall()]

async def db_update_account(aid: int, **kw):
    sets = ", ".join(f"{k}=?" for k in kw)
    await _db.execute(f"UPDATE accounts SET {sets} WHERE id=?", (*kw.values(), aid))
    await _db.commit()

async def db_delete_account(aid: int):
    await _db.execute("DELETE FROM accounts WHERE id=?", (aid,))
    await _db.commit()

async def db_increment_account_stat(aid: int, field: str):
    await _db.execute(f"UPDATE accounts SET {field}={field}+1, last_used=datetime('now') WHERE id=?", (aid,))
    await _db.commit()

# ─── Tasks ───

async def db_add_task(name, account_id, task_type, target, content=None, parse_mode="markdown", created_by=None) -> int:
    await _db.execute(
        "INSERT INTO tasks (name,account_id,task_type,target,content,parse_mode,created_by) VALUES (?,?,?,?,?,?,?)",
        (name, account_id, task_type, target, content, parse_mode, created_by)
    )
    await _db.commit()
    cur = await _db.execute("SELECT last_insert_rowid()")
    return (await cur.fetchone())[0]

async def db_get_task(tid: int) -> Optional[dict]:
    cur = await _db.execute("SELECT * FROM tasks WHERE id=?", (tid,))
    row = await cur.fetchone()
    return dict(row) if row else None

async def db_get_all_tasks(account_id: int = None) -> List[dict]:
    if account_id:
        cur = await _db.execute(
            "SELECT t.*, a.phone, a.username, a.full_name FROM tasks t JOIN accounts a ON t.account_id=a.id WHERE t.is_active=1 AND t.account_id=? ORDER BY t.id DESC",
            (account_id,)
        )
    else:
        cur = await _db.execute(
            "SELECT t.*, a.phone, a.username, a.full_name FROM tasks t JOIN accounts a ON t.account_id=a.id WHERE t.is_active=1 ORDER BY t.id DESC"
        )
    return [dict(r) for r in await cur.fetchall()]

async def db_delete_task(tid: int):
    await _db.execute("DELETE FROM tasks WHERE id=?", (tid,))
    await _db.commit()

async def db_update_task(tid: int, **kw):
    sets = ", ".join(f"{k}=?" for k in kw)
    await _db.execute(f"UPDATE tasks SET {sets} WHERE id=?", (*kw.values(), tid))
    await _db.commit()

# ─── Schedules ───

async def db_add_schedule(task_id, trigger_type, trigger_data, max_runs=-1) -> int:
    await _db.execute(
        "INSERT INTO schedules (task_id,trigger_type,trigger_data,max_runs) VALUES (?,?,?,?)",
        (task_id, trigger_type, json.dumps(trigger_data), max_runs)
    )
    await _db.commit()
    cur = await _db.execute("SELECT last_insert_rowid()")
    return (await cur.fetchone())[0]

async def db_get_active_schedules() -> List[dict]:
    cur = await _db.execute("""
        SELECT s.*, t.name as task_name, t.account_id, t.task_type,
               t.target, t.content, t.parse_mode
        FROM schedules s JOIN tasks t ON s.task_id=t.id
        WHERE s.is_active=1 AND t.is_active=1
          AND (s.max_runs=-1 OR s.run_count < s.max_runs)
        ORDER BY s.id
    """)
    rows = await cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["trigger_data"] = json.loads(d["trigger_data"])
        result.append(d)
    return result

async def db_update_schedule_run(sid: int):
    await _db.execute(
        "UPDATE schedules SET last_run=datetime('now'), run_count=run_count+1 WHERE id=?", (sid,)
    )
    await _db.commit()

async def db_delete_schedule(sid: int):
    await _db.execute("DELETE FROM schedules WHERE id=?", (sid,))
    await _db.commit()

# ─── Logs ───

async def db_log(task_id, schedule_id, account_id, status, message=None, duration_ms=0):
    await _db.execute(
        "INSERT INTO execution_logs (task_id,schedule_id,account_id,status,message,duration_ms) VALUES (?,?,?,?,?,?)",
        (task_id, schedule_id, account_id, status, message, duration_ms)
    )
    await _db.commit()

async def db_get_logs(limit=20, account_id=None, status=None) -> List[dict]:
    conditions = []
    params = []
    if account_id:
        conditions.append("l.account_id=?")
        params.append(account_id)
    if status:
        conditions.append("l.status=?")
        params.append(status)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)
    cur = await _db.execute(
        f"SELECT l.*, t.name as task_name, a.phone FROM execution_logs l "
        f"LEFT JOIN tasks t ON l.task_id=t.id "
        f"LEFT JOIN accounts a ON l.account_id=a.id "
        f"{where} ORDER BY l.executed_at DESC LIMIT ?",
        params
    )
    return [dict(r) for r in await cur.fetchall()]

async def db_get_stats() -> dict:
    stats = {}
    queries = {
        "total_accounts":  "SELECT COUNT(*) FROM accounts",
        "active_accounts": "SELECT COUNT(*) FROM accounts WHERE is_active=1 AND is_banned=0",
        "banned_accounts": "SELECT COUNT(*) FROM accounts WHERE is_banned=1",
        "total_tasks":     "SELECT COUNT(*) FROM tasks WHERE is_active=1",
        "total_schedules": "SELECT COUNT(*) FROM schedules WHERE is_active=1",
        "total_msgs":      "SELECT COALESCE(SUM(msg_count),0) FROM accounts",
        "ok_today":        "SELECT COUNT(*) FROM execution_logs WHERE status='success' AND date(executed_at)=date('now')",
        "fail_today":      "SELECT COUNT(*) FROM execution_logs WHERE status='failed'  AND date(executed_at)=date('now')",
        "ok_total":        "SELECT COUNT(*) FROM execution_logs WHERE status='success'",
        "fail_total":      "SELECT COUNT(*) FROM execution_logs WHERE status='failed'",
        "total_logs":      "SELECT COUNT(*) FROM execution_logs",
    }
    for k, q in queries.items():
        cur = await _db.execute(q)
        stats[k] = (await cur.fetchone())[0]
    return stats

# ─── Notifications ───

async def db_add_notification(ntype: str, title: str, body: str = None):
    await _db.execute(
        "INSERT INTO notifications (type,title,body) VALUES (?,?,?)",
        (ntype, title, body)
    )
    await _db.commit()

async def db_get_unread_notifications() -> List[dict]:
    cur = await _db.execute(
        "SELECT * FROM notifications WHERE is_read=0 ORDER BY created_at DESC LIMIT 10"
    )
    return [dict(r) for r in await cur.fetchall()]

async def db_mark_notifications_read():
    await _db.execute("UPDATE notifications SET is_read=1")
    await _db.commit()

# ─── Blacklist ───

async def db_add_blacklist(target: str, reason: str = None):
    await _db.execute(
        "INSERT OR IGNORE INTO blacklist (target,reason) VALUES (?,?)", (target, reason)
    )
    await _db.commit()

async def db_get_blacklist() -> List[dict]:
    cur = await _db.execute("SELECT * FROM blacklist ORDER BY added_at DESC")
    return [dict(r) for r in await cur.fetchall()]

async def db_remove_blacklist(target: str):
    await _db.execute("DELETE FROM blacklist WHERE target=?", (target,))
    await _db.commit()

async def db_is_blacklisted(target: str) -> bool:
    cur = await _db.execute("SELECT id FROM blacklist WHERE target=?", (target,))
    return (await cur.fetchone()) is not None

# ══════════════════════════════════════════════
#  ⚡ Rate Limiter
# ══════════════════════════════════════════════

class TokenBucket:
    def __init__(self, capacity, rate):
        self.capacity = capacity
        self.tokens   = float(capacity)
        self.rate     = rate
        self.last     = time.monotonic()
        self._lock    = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            wait = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait)
            self.tokens = 0

    def available(self) -> float:
        now = time.monotonic()
        return min(self.capacity, self.tokens + (now - self.last) * self.rate)

_buckets: Dict[int, TokenBucket] = {}

def get_bucket(aid: int) -> TokenBucket:
    if aid not in _buckets:
        _buckets[aid] = TokenBucket(RATE_MSGS, RATE_MSGS / RATE_PERIOD)
    return _buckets[aid]

# ══════════════════════════════════════════════
#  🛡 Anti-Ban
# ══════════════════════════════════════════════

_last_action: Dict[int, float] = {}
_last_join:   Dict[int, float] = {}
_flood_until: Dict[int, float] = {}

async def smart_delay(aid: int, mn=None, mx=None):
    """تأخير ذكي يحاكي السلوك البشري"""
    # انتظر انتهاء Flood إن وجد
    if aid in _flood_until:
        remaining = _flood_until[aid] - time.monotonic()
        if remaining > 0:
            logger.info(f"[{aid}] FloodWait: {remaining:.0f}s")
            await asyncio.sleep(remaining)
        del _flood_until[aid]

    mn = MIN_DELAY if mn is None else mn
    mx = MAX_DELAY if mx is None else mx
    delay = random.uniform(mn, mx)

    # Jitter إضافي أحيانًا
    if random.random() < 0.15:
        delay += random.uniform(2, 6)

    # احترم الحد الأدنى منذ آخر عملية
    last = _last_action.get(aid, 0)
    elapsed = time.monotonic() - last
    if elapsed < mn:
        delay += mn - elapsed

    await asyncio.sleep(delay)
    _last_action[aid] = time.monotonic()

async def join_delay(aid: int):
    """تأخير خاص بالانضمام"""
    last = _last_join.get(aid, 0)
    elapsed = time.monotonic() - last
    if elapsed < JOIN_DELAY:
        wait = JOIN_DELAY - elapsed + random.uniform(5, 20)
        logger.info(f"[{aid}] تأخير الانضمام: {wait:.0f}s")
        await asyncio.sleep(wait)
    _last_join[aid] = time.monotonic()

async def typing_sim(text: str):
    """محاكاة الكتابة"""
    speed = random.uniform(180, 380)
    t = min(max(len(text) / speed * 60, 0.5), 25.0)
    await asyncio.sleep(t)

def humanize(text: str) -> str:
    """تنويع بسيط في الرسالة"""
    return random.choices(
        [text, text + " ", text],
        weights=[75, 15, 10]
    )[0]

def classify_error(e: Exception) -> str:
    s = str(e).lower()
    if "banned" in s or "deactivated" in s: return "BANNED"
    if "flood" in s:    return "FLOOD"
    if "spam" in s:     return "SPAM"
    if "forbidden" in s: return "FORBIDDEN"
    if "connection" in s or "timeout" in s: return "CONN"
    if "not found" in s or "invalid" in s:  return "INVALID"
    return "OTHER"

def retry_delay_seconds(etype: str, attempt: int) -> float:
    base = {"FLOOD":60,"SPAM":300,"CONN":5,"OTHER":10,"FORBIDDEN":30}.get(etype,10)
    return min(base * (2**(attempt-1)) + random.uniform(0, base*0.1), 3600)

# ══════════════════════════════════════════════
#  📱 UserBot Client
# ══════════════════════════════════════════════

class UserBotClient:
    def __init__(self, aid, phone, api_id, api_hash, session_name):
        self.account_id   = aid
        self.phone        = phone
        self.api_id       = api_id
        self.api_hash     = api_hash
        self.session_name = session_name
        self.session_path = str(SESSIONS_DIR / session_name)
        self._client: Optional[TelegramClient] = None
        self._me          = None
        self._connected   = False
        self._wd_task     = None
        self.connect_time: Optional[datetime] = None
        self.reconnect_count = 0

    async def connect(self) -> bool:
        try:
            self._client = TelegramClient(
                self.session_path, self.api_id, self.api_hash,
                flood_sleep_threshold=60, connection_retries=5,
                retry_delay=1, auto_reconnect=True,
                device_model="Samsung Galaxy S23",
                system_version="Android 14",
                app_version="10.3.2",
                lang_code="ar",
            )
            await self._client.connect()
            if not await self._client.is_user_authorized():
                logger.warning(f"[{self.phone}] يحتاج تسجيل دخول")
                return False
            self._me = await self._client.get_me()
            self._connected = True
            self.connect_time = datetime.now()
            self._start_watchdog()
            logger.info(f"✅ [{self.phone}] {self._me.first_name} - متصل")
            return True
        except AuthKeyError:
            logger.error(f"[{self.phone}] جلسة منتهية")
            return False
        except Exception as e:
            logger.error(f"[{self.phone}] فشل: {e}")
            return False

    async def disconnect(self):
        if self._wd_task:
            self._wd_task.cancel()
            try: await self._wd_task
            except asyncio.CancelledError: pass
        if self._client:
            try: await self._client.disconnect()
            except Exception: pass
        self._connected = False

    def _start_watchdog(self):
        if self._wd_task and not self._wd_task.done():
            return
        self._wd_task = asyncio.create_task(self._watchdog())

    async def _watchdog(self):
        while True:
            await asyncio.sleep(45)
            if self._client and not self._client.is_connected():
                logger.warning(f"[{self.phone}] انقطع - إعادة اتصال...")
                self.reconnect_count += 1
                try:
                    await self._client.connect()
                    if await self._client.is_user_authorized():
                        self._connected = True
                        logger.info(f"[{self.phone}] ✅ أُعيد الاتصال (#{self.reconnect_count})")
                        await db_add_notification("reconnect", f"حساب أُعيد اتصاله", self.phone)
                    else:
                        self._connected = False
                        await db_update_account(self.account_id, is_active=0)
                        break
                except Exception as e:
                    logger.error(f"[{self.phone}] فشل: {e}")
                    await asyncio.sleep(90)

    async def send_code(self) -> str:
        if not self._client:
            self._client = TelegramClient(self.session_path, self.api_id, self.api_hash)
            await self._client.connect()
        r = await self._client.send_code_request(self.phone)
        return r.phone_code_hash

    async def sign_in(self, code: str, hash_: str, password: str = None) -> bool:
        try:
            await self._client.sign_in(self.phone, code, phone_code_hash=hash_)
            self._me = await self._client.get_me()
            self._connected = True
            self.connect_time = datetime.now()
            self._start_watchdog()
            return True
        except SessionPasswordNeededError:
            if password:
                await self._client.sign_in(password=password)
                self._me = await self._client.get_me()
                self._connected = True
                self.connect_time = datetime.now()
                self._start_watchdog()
                return True
            return False
        except PhoneCodeInvalidError:
            return False

    @property
    def is_connected(self):
        return self._connected and bool(self._client) and self._client.is_connected()

    @property
    def me(self): return self._me

    @property
    def client(self): return self._client

    def display_name(self) -> str:
        if self._me:
            return f"{self._me.first_name or ''} {self._me.last_name or ''}".strip() or self.phone
        return self.phone

    def uptime(self) -> str:
        if not self.connect_time: return "—"
        diff = datetime.now() - self.connect_time
        h, r = divmod(int(diff.total_seconds()), 3600)
        m, s = divmod(r, 60)
        return f"{h}س {m}د {s}ث"

    def info_dict(self) -> dict:
        return {
            "id":        self.account_id,
            "phone":     self.phone,
            "name":      self.display_name(),
            "username":  f"@{self._me.username}" if self._me and self._me.username else "—",
            "connected": self.is_connected,
            "uptime":    self.uptime(),
            "reconnects": self.reconnect_count,
        }

    async def connect_with_string(self, session_string: str) -> bool:
        """الاتصال باستخدام Session String مباشرة بدون ملف جلسة"""
        try:
            self._client = TelegramClient(
                StringSession(session_string), self.api_id, self.api_hash,
                flood_sleep_threshold=60, connection_retries=5,
                retry_delay=1, auto_reconnect=True,
                device_model="Samsung Galaxy S23",
                system_version="Android 14",
                app_version="10.3.2",
                lang_code="ar",
            )
            await self._client.connect()
            if not await self._client.is_user_authorized():
                logger.warning(f"[{self.phone}] Session String غير مصرح")
                return False
            self._me = await self._client.get_me()
            self._connected = True
            self.connect_time = datetime.now()
            self._start_watchdog()
            logger.info(f"✅ [Session String #{self.account_id}] {self._me.first_name} - متصل")
            return True
        except AuthKeyError:
            logger.error(f"[Session String #{self.account_id}] مفتاح منتهي أو غير صالح")
            return False
        except Exception as e:
            logger.error(f"[Session String #{self.account_id}] فشل الاتصال: {e}")
            return False

# ══════════════════════════════════════════════
#  ⚡ UserBot Actions
# ══════════════════════════════════════════════

class UserBotActions:
    MAX_RETRIES = 3

    def __init__(self, ub: UserBotClient):
        self.ub = ub

    async def send_message(self, target, text, parse_mode="markdown", typing=True, reply_to=None) -> bool:
        if not self.ub.is_connected: return False
        if await db_is_blacklisted(str(target)):
            logger.warning(f"[{self.ub.phone}] {target} في القائمة السوداء")
            return False
        await get_bucket(self.ub.account_id).acquire()
        await smart_delay(self.ub.account_id)
        if typing:
            try:
                async with self.ub.client.action(target, "typing"):
                    await typing_sim(text)
            except Exception: pass
        final = humanize(text)
        t0 = time.monotonic()
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                await self.ub.client.send_message(
                    target, final, parse_mode=parse_mode, reply_to=reply_to
                )
                ms = int((time.monotonic() - t0) * 1000)
                await db_increment_account_stat(self.ub.account_id, "msg_count")
                logger.info(f"[{self.ub.phone}] ✅ → {target} ({ms}ms)")
                return True
            except FloodWaitError as e:
                _flood_until[self.ub.account_id] = time.monotonic() + e.seconds + 5
                await asyncio.sleep(e.seconds + 5)
            except PeerFloodError:
                await db_update_account(self.ub.account_id, is_banned=1)
                await db_add_notification("ban", "حساب في خطر", f"{self.ub.phone} - PeerFloodError")
                return False
            except (ChatWriteForbiddenError, UserBannedInChannelError):
                return False
            except Exception as e:
                etype = classify_error(e)
                if etype == "BANNED":
                    await db_update_account(self.ub.account_id, is_banned=1)
                    return False
                if attempt < self.MAX_RETRIES:
                    await asyncio.sleep(retry_delay_seconds(etype, attempt))
        return False

    async def send_to_many(self, targets: list, text: str, parse_mode="markdown") -> Dict[str, str]:
        results = {}
        for i, t in enumerate(targets):
            ok = await self.send_message(t, text, parse_mode)
            results[str(t)] = "success" if ok else "failed"
            if i < len(targets) - 1:
                await smart_delay(self.ub.account_id, 5, 20)
        return results

    async def join_group(self, target: str) -> bool:
        await join_delay(self.ub.account_id)
        if await db_is_blacklisted(target):
            logger.warning(f"[{self.ub.phone}] {target} في القائمة السوداء")
            return False
        try:
            if "t.me/+" in target or "t.me/joinchat/" in target:
                h = target.split("/")[-1].replace("+","")
                await self.ub.client(ImportChatInviteRequest(h))
            else:
                entity = await self.ub.client.get_entity(target.lstrip("@"))
                await self.ub.client(JoinChannelRequest(entity))
            await db_increment_account_stat(self.ub.account_id, "join_count")
            logger.info(f"[{self.ub.phone}] ✅ انضم: {target}")
            return True
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5)
            return False
        except Exception as e:
            logger.error(f"[{self.ub.phone}] فشل join {target}: {e}")
            return False

    async def leave_group(self, target) -> bool:
        try:
            entity = await self.ub.client.get_entity(target)
            await self.ub.client(LeaveChannelRequest(entity))
            return True
        except Exception as e:
            logger.error(f"[{self.ub.phone}] فشل leave: {e}")
            return False

    async def forward_message(self, from_chat, msg_id: int, to_chat) -> bool:
        try:
            await self.ub.client.forward_messages(to_chat, msg_id, from_chat)
            return True
        except Exception as e:
            logger.error(f"[{self.ub.phone}] فشل forward: {e}")
            return False

    async def get_dialogs(self, limit=20) -> list:
        try:
            return await self.ub.client.get_dialogs(limit=limit)
        except Exception: return []

    async def get_entity_info(self, target) -> Optional[dict]:
        try:
            e = await self.ub.client.get_entity(target)
            return {
                "id":       e.id,
                "type":     type(e).__name__,
                "title":    getattr(e,"title",None) or getattr(e,"first_name","?"),
                "username": getattr(e,"username",None),
                "verified": getattr(e,"verified",False),
            }
        except Exception: return None

    async def get_messages(self, chat, limit=5) -> list:
        try:
            msgs = await self.ub.client.get_messages(chat, limit=limit)
            return [{"id":m.id,"text":(m.text or "")[:80],"date":str(m.date)} for m in msgs]
        except Exception: return []

    async def delete_message(self, chat, msg_id: int, revoke=True) -> bool:
        try:
            await self.ub.client.delete_messages(chat, [msg_id], revoke=revoke)
            return True
        except Exception: return False

    async def edit_message(self, chat, msg_id: int, text: str) -> bool:
        try:
            await self.ub.client.edit_message(chat, msg_id, text)
            return True
        except Exception: return False

# ══════════════════════════════════════════════
#  👥 Account Manager
# ══════════════════════════════════════════════

_clients:        Dict[int, UserBotClient]  = {}
_actions:        Dict[int, UserBotActions] = {}
_login_sessions: Dict[str, dict]           = {}
_mgr_lock = asyncio.Lock()

async def mgr_load_all():
    # ── تحميل الحسابات من قاعدة البيانات ──
    accounts = await db_get_all_accounts(active_only=True)
    logger.info(f"تحميل {len(accounts)} حساب من قاعدة البيانات...")
    db_results = []
    if accounts:
        db_results = await asyncio.gather(*[_start_acc(a) for a in accounts], return_exceptions=True)
        ok_db = sum(1 for r in db_results if r is True)
        logger.info(f"✅ {ok_db}/{len(accounts)} حساب DB متصل")
    else:
        logger.info("لا توجد حسابات في قاعدة البيانات")

    # ── تحميل الحسابات من SESSION_ متغيرات البيئة ──
    if SESSION_ACCOUNTS:
        logger.info(f"🔑 وجدت {len(SESSION_ACCOUNTS)} Session String في متغيرات البيئة...")
        env_results = await asyncio.gather(
            *[load_session_from_env(num, ss) for num, ss in SESSION_ACCOUNTS.items()],
            return_exceptions=True
        )
        ok_env = sum(1 for r in env_results if r is True)
        logger.info(f"✅ {ok_env}/{len(SESSION_ACCOUNTS)} Session String متصل")
    else:
        logger.info("لا توجد Session Strings في متغيرات البيئة")

async def load_session_from_env(session_num: str, session_string: str) -> bool:
    """تحميل حساب من Session String في متغيرات البيئة"""
    # توليد ID وهمي فريد للحسابات البيئية (سالب لتمييزها)
    fake_id = -(abs(hash(session_string)) % 999999 + 1)
    # تجنب التكرار إذا كان محمّلاً بالفعل
    if fake_id in _clients:
        return _clients[fake_id].is_connected
    phone_label = f"ENV_SESSION_{session_num}"
    ub = UserBotClient(fake_id, phone_label, DEFAULT_API_ID, DEFAULT_API_HASH, f"env_session_{session_num}")
    try:
        if await ub.connect_with_string(session_string):
            async with _mgr_lock:
                _clients[fake_id] = ub
                _actions[fake_id] = UserBotActions(ub)
            logger.info(f"✅ SESSION_{session_num} ({ub.display_name()}) متصل — ID: {fake_id}")
            return True
        else:
            logger.warning(f"⚠️ SESSION_{session_num} فشل الاتصال")
            return False
    except Exception as e:
        logger.error(f"❌ SESSION_{session_num} خطأ: {e}")
        return False

async def _start_acc(acc: dict) -> bool:
    aid = acc["id"]
    try:
        ub = UserBotClient(aid, acc["phone"], int(acc["api_id"]), acc["api_hash"], acc["session_name"])
        if await ub.connect():
            async with _mgr_lock:
                _clients[aid] = ub
                _actions[aid] = UserBotActions(ub)
            return True
    except Exception as e:
        logger.error(f"[{aid}] {e}")
    return False

async def mgr_stop_all():
    for ub in list(_clients.values()):
        await ub.disconnect()
    _clients.clear()
    _actions.clear()

async def mgr_begin_login(phone, api_id, api_hash) -> dict:
    sname = f"acc_{phone.replace('+','').replace(' ','')}"
    ub = UserBotClient(0, phone, api_id, api_hash, sname)
    try:
        h = await ub.send_code()
        _login_sessions[phone] = {"ub":ub,"hash":h,"api_id":api_id,"api_hash":api_hash,"sname":sname}
        return {"status":"code_sent"}
    except Exception as e:
        return {"status":"error","message":str(e)}

async def mgr_complete_login(phone, code, password=None, notes=None) -> dict:
    sess = _login_sessions.get(phone)
    if not sess: return {"status":"error","message":"لا توجد جلسة نشطة"}
    ub: UserBotClient = sess["ub"]
    try:
        ok = await ub.sign_in(code, sess["hash"], password)
        if not ok: return {"status":"need_2fa"}
        me = ub.me
        aid = await db_add_account(
            phone, str(sess["api_id"]), sess["api_hash"], sess["sname"],
            username=me.username,
            full_name=f"{me.first_name or ''} {me.last_name or ''}".strip(),
            notes=notes,
        )
        ub.account_id = aid
        async with _mgr_lock:
            _clients[aid] = ub
            _actions[aid] = UserBotActions(ub)
        _login_sessions.pop(phone, None)
        await db_add_notification("new_account", "حساب جديد أُضيف", ub.display_name())
        return {"status":"success","account_id":aid,"name":ub.display_name(),"username":me.username}
    except Exception as e:
        return {"status":"error","message":str(e)}

async def mgr_remove(aid: int):
    if aid in _clients:
        await _clients[aid].disconnect()
        _clients.pop(aid, None)
        _actions.pop(aid, None)
    await db_delete_account(aid)

def mgr_actions(aid: int) -> Optional[UserBotActions]:
    return _actions.get(aid)

def mgr_client(aid: int) -> Optional[UserBotClient]:
    return _clients.get(aid)

def mgr_is_connected(aid: int) -> bool:
    c = _clients.get(aid)
    return c.is_connected if c else False

def mgr_all_connected() -> List[int]:
    return [aid for aid, c in _clients.items() if c.is_connected]

def mgr_count() -> int:
    return len(mgr_all_connected())

def mgr_all_status() -> List[dict]:
    return [c.info_dict() for c in _clients.values()]

def mgr_get_all_for_task() -> List[dict]:
    """جلب كل الحسابات المتصلة (DB + Session Strings) لاختيارها في المهام"""
    result = []
    for aid, ub in _clients.items():
        if not ub.is_connected:
            continue
        result.append({
            "id":        aid,
            "phone":     ub.phone,
            "full_name": ub.display_name(),
            "username":  ub._me.username if ub._me else None,
            "is_env":    aid < 0,
        })
    return result

# ══════════════════════════════════════════════
#  📅 Scheduler
# ══════════════════════════════════════════════

_scheduler = AsyncIOScheduler(timezone=TZ)

def _build_trigger(ttype, tdata):
    try:
        if ttype == "once":
            return DateTrigger(run_date=datetime.fromisoformat(tdata["datetime"]), timezone=TZ)
        elif ttype == "interval":
            kw = {k:int(v) for k,v in tdata.items() if k in ["weeks","days","hours","minutes","seconds"]}
            return IntervalTrigger(timezone=TZ, **(kw or {"hours":1}))
        elif ttype == "cron":
            p = tdata.get("expression","0 9 * * *").split()
            if len(p) == 5:
                return CronTrigger(minute=p[0],hour=p[1],day=p[2],month=p[3],day_of_week=p[4],timezone=TZ)
    except Exception as e:
        logger.error(f"Trigger error: {e}")
    return None

async def _run_task(sid, tid, aid, task_type, target, content, parse_mode):
    t0 = time.monotonic()
    logger.info(f"▶️ تنفيذ مهمة {tid} (جدول {sid})")
    status, msg = "failed", ""
    actions = mgr_actions(aid)
    if not actions:
        msg = f"الحساب {aid} غير متصل"
    else:
        try:
            if task_type == "send_message":
                ok = await actions.send_message(target, content or "", parse_mode)
            elif task_type == "join_group":
                ok = await actions.join_group(target)
            elif task_type == "leave_group":
                ok = await actions.leave_group(target)
            elif task_type == "forward":
                parts = (content or "").split(":")
                ok = await actions.forward_message(parts[0], int(parts[1]), target) if len(parts)==2 else False
            else:
                ok = False
            status = "success" if ok else "failed"
            await _db.execute("UPDATE tasks SET run_count=run_count+1 WHERE id=?", (tid,))
            await _db.commit()
        except Exception as e:
            msg = str(e)
    ms = int((time.monotonic()-t0)*1000)
    await db_log(tid, sid, aid, status, msg, ms)
    await db_update_schedule_run(sid)
    icon = "✅" if status=="success" else "❌"
    logger.info(f"{icon} مهمة {tid}: {status} ({ms}ms)")

async def sched_add(tid, ttype, tdata, max_runs=-1) -> int:
    task = await db_get_task(tid)
    if not task: raise ValueError("مهمة غير موجودة")
    sid = await db_add_schedule(tid, ttype, tdata, max_runs)
    trigger = _build_trigger(ttype, tdata)
    if trigger:
        _scheduler.add_job(
            _run_task, trigger, id=f"s{sid}",
            name=task["name"],
            kwargs=dict(sid=sid,tid=tid,aid=task["account_id"],
                        task_type=task["task_type"],target=task["target"],
                        content=task.get("content",""),parse_mode=task.get("parse_mode","markdown")),
            replace_existing=True, max_instances=1, misfire_grace_time=300,
        )
    return sid

async def sched_remove(sid: int):
    await db_delete_schedule(sid)
    try: _scheduler.remove_job(f"s{sid}")
    except Exception: pass

async def sched_load_all():
    schedules = await db_get_active_schedules()
    for s in schedules:
        trigger = _build_trigger(s["trigger_type"], s["trigger_data"])
        if trigger:
            _scheduler.add_job(
                _run_task, trigger, id=f"s{s['id']}",
                name=s.get("task_name","Task"),
                kwargs=dict(sid=s["id"],tid=s["task_id"],aid=s["account_id"],
                            task_type=s["task_type"],target=s["target"],
                            content=s.get("content",""),parse_mode=s.get("parse_mode","markdown")),
                replace_existing=True, max_instances=1, misfire_grace_time=300,
            )
    logger.info(f"✅ {len(schedules)} جدول محمّل")

def sched_jobs() -> list:
    return [{"id":j.id,"name":j.name,"next":str(j.next_run_time)} for j in _scheduler.get_jobs()]

# ══════════════════════════════════════════════
#  🎛 Keyboards
# ══════════════════════════════════════════════

def kb_main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👤 الحسابات",    callback_data="menu_accounts"),
        InlineKeyboardButton("⚙️ المهام",       callback_data="menu_tasks"),
        InlineKeyboardButton("📅 الجداول",      callback_data="menu_schedules"),
        InlineKeyboardButton("📊 الإحصائيات",  callback_data="menu_stats"),
        InlineKeyboardButton("📋 السجلات",     callback_data="menu_logs"),
        InlineKeyboardButton("🛡 الحماية",      callback_data="menu_protection"),
    )
    kb.add(InlineKeyboardButton("🔔 الإشعارات", callback_data="menu_notifications"))
    return kb

def kb_accounts_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📋 عرض الكل",      callback_data="acc_list"),
        InlineKeyboardButton("➕ إضافة حساب",    callback_data="acc_add"),
        InlineKeyboardButton("📊 إحصائيات",     callback_data="acc_stats"),
        InlineKeyboardButton("◀️ رجوع",          callback_data="back_main"),
    )
    return kb

def kb_tasks_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📋 عرض المهام",    callback_data="task_list"),
        InlineKeyboardButton("➕ مهمة جديدة",   callback_data="task_add"),
        InlineKeyboardButton("◀️ رجوع",          callback_data="back_main"),
    )
    return kb

def kb_account_actions(aid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 التفاصيل",       callback_data=f"acc_detail_{aid}"),
        InlineKeyboardButton("💬 محادثاته",       callback_data=f"acc_dialogs_{aid}"),
        InlineKeyboardButton("⚙️ مهامه",          callback_data=f"acc_tasks_{aid}"),
        InlineKeyboardButton("🔄 إعادة اتصال",   callback_data=f"acc_reconnect_{aid}"),
        InlineKeyboardButton("🗑 حذف",            callback_data=f"acc_delete_{aid}"),
        InlineKeyboardButton("◀️ رجوع",           callback_data="acc_list"),
    )
    return kb

def kb_task_actions(tid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("▶️ تشغيل الآن",    callback_data=f"task_run_{tid}"),
        InlineKeyboardButton("📅 جدولة",          callback_data=f"task_sched_{tid}"),
        InlineKeyboardButton("✏️ تعديل",          callback_data=f"task_edit_{tid}"),
        InlineKeyboardButton("🗑 حذف",            callback_data=f"task_delete_{tid}"),
        InlineKeyboardButton("◀️ رجوع",           callback_data="task_list"),
    )
    return kb

def kb_confirm(yes_data: str, no_data: str = "cancel") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ نعم، تأكيد", callback_data=yes_data),
        InlineKeyboardButton("❌ إلغاء",       callback_data=no_data),
    )
    return kb

def kb_back(to: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("◀️ رجوع", callback_data=to))
    return kb

def kb_pagination(current: int, total: int, prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    btns = []
    if current > 1:
        btns.append(InlineKeyboardButton("◀️", callback_data=f"{prefix}_p{current-1}"))
    btns.append(InlineKeyboardButton(f"📄 {current}/{total}", callback_data="noop"))
    if current < total:
        btns.append(InlineKeyboardButton("▶️", callback_data=f"{prefix}_p{current+1}"))
    if btns: kb.row(*btns)
    return kb

# ══════════════════════════════════════════════
#  🤖 Control Bot
# ══════════════════════════════════════════════

bot  = telebot.TeleBot(BOT_TOKEN, parse_mode=None) if BOT_TOKEN else None
_loop: Optional[asyncio.AbstractEventLoop] = None

def arun(coro):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=120)

def is_admin(msg_or_call) -> bool:
    uid = msg_or_call.from_user.id if hasattr(msg_or_call,"from_user") else msg_or_call
    return uid in ADMIN_IDS

def edit_or_send(call: CallbackQuery, text: str, kb=None, parse_mode="Markdown"):
    try:
        bot.edit_message_text(
            text, call.message.chat.id, call.message.message_id,
            parse_mode=parse_mode, reply_markup=kb
        )
    except Exception:
        bot.send_message(call.message.chat.id, text, parse_mode=parse_mode, reply_markup=kb)

# حالات المحادثات
_states: Dict[int, dict] = {}

# ── States helper ──
def set_state(uid: int, state: dict): _states[uid] = state
def get_state(uid: int) -> dict:      return _states.get(uid, {})
def clear_state(uid: int):            _states.pop(uid, None)
def in_state(uid: int, step: str) -> bool:
    return _states.get(uid, {}).get("step") == step

def setup_bot():
    if not bot: return

    # ══════════════════════════════════════════
    #  /start - القائمة الرئيسية
    # ══════════════════════════════════════════

    @bot.message_handler(commands=["start","menu"])
    def cmd_start(msg: Message):
        if not is_admin(msg): return
        notifications = arun(db_get_unread_notifications())
        notif_badge = f" 🔔{len(notifications)}" if notifications else ""
        text = (
            f"`{'━'*28}`\n"
            f"  {S.BOT} *{BOT_NAME}*  `v{VERSION}`\n"
            f"`{'━'*28}`\n\n"
            f"{S.ONLINE} متصل: `{mgr_count()}`  "
            f"{S.SCHEDULE} جداول: `{len(sched_jobs())}`{notif_badge}\n\n"
            f"اختر من القائمة 👇"
        )
        bot.send_message(msg.chat.id, text, parse_mode="Markdown", reply_markup=kb_main_menu())

    # ── Callback: القوائم الرئيسية ──

    @bot.callback_query_handler(func=lambda c: c.data == "back_main")
    def cb_main(call: CallbackQuery):
        if not is_admin(call): return
        text = (
            f"`{'━'*28}`\n"
            f"  {S.BOT} *{BOT_NAME}*\n"
            f"`{'━'*28}`\n\n"
            f"{S.ONLINE} متصل: `{mgr_count()}`  {S.SCHEDULE} جداول: `{len(sched_jobs())}`\n\n"
            f"اختر من القائمة 👇"
        )
        edit_or_send(call, text, kb_main_menu())
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "noop")
    def cb_noop(call: CallbackQuery):
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "cancel")
    def cb_cancel(call: CallbackQuery):
        clear_state(call.from_user.id)
        edit_or_send(call, "❌ *تم الإلغاء*", kb_back("back_main"))
        bot.answer_callback_query(call.id)

    # ══════════════════════════════════════════
    #  👤 الحسابات
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_accounts")
    def cb_menu_accounts(call: CallbackQuery):
        if not is_admin(call): return
        connected = mgr_count()
        accounts = arun(db_get_all_accounts(active_only=False))
        total = len(accounts)
        text = (
            f"{S.header('إدارة الحسابات', S.ACCOUNT)}\n\n"
            f"{S.field('إجمالي الحسابات', total)}\n"
            f"{S.field('متصل الآن', connected, S.ONLINE)}\n"
            f"{S.field('منفصل', total-connected, S.OFFLINE)}\n\n"
            f"اختر ما تريد 👇"
        )
        edit_or_send(call, text, kb_accounts_menu())
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "acc_list")
    def cb_acc_list(call: CallbackQuery):
        if not is_admin(call): return
        accounts = arun(db_get_all_accounts(active_only=False))
        if not accounts:
            text = (
                f"{S.header('الحسابات', S.ACCOUNT)}\n\n"
                f"📭 _لا توجد حسابات مضافة_\n\n"
                f"اضغط ➕ لإضافة حساب جديد"
            )
            edit_or_send(call, text, kb_accounts_menu())
            bot.answer_callback_query(call.id)
            return

        lines = [f"{S.header('قائمة الحسابات', S.ACCOUNT)}\n"]
        for a in accounts:
            connected = mgr_is_connected(a["id"])
            badge = S.status_badge(connected, bool(a["is_banned"]))
            name = a.get("full_name") or a["phone"]
            uname = f"@{a['username']}" if a.get("username") else ""
            lines.append(
                f"\n{badge}\n"
                f"  {S.ID} `{a['id']}` • *{name}* {uname}\n"
                f"  {S.PHONE} `{a['phone']}`\n"
                f"  {S.CHART} رسائل: `{a.get('msg_count',0)}` • انضمام: `{a.get('join_count',0)}`"
            )

        kb = InlineKeyboardMarkup(row_width=3)
        for a in accounts:
            connected = mgr_is_connected(a["id"])
            icon = "🟢" if connected else "🔴"
            kb.add(InlineKeyboardButton(
                f"{icon} {a.get('full_name') or a['phone'][:15]}",
                callback_data=f"acc_detail_{a['id']}"
            ))
        kb.add(
            InlineKeyboardButton("➕ إضافة", callback_data="acc_add"),
            InlineKeyboardButton("◀️ رجوع",  callback_data="menu_accounts"),
        )
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_detail_"))
    def cb_acc_detail(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_")[-1])
        acc = arun(db_get_account(aid))
        if not acc:
            bot.answer_callback_query(call.id, "❌ الحساب غير موجود")
            return
        ub = mgr_client(aid)
        connected = mgr_is_connected(aid)
        badge = S.status_badge(connected, bool(acc["is_banned"]))
        text = (
            f"{S.header('تفاصيل الحساب', S.ACCOUNT)}\n\n"
            f"{S.ID} *ID:* `{acc['id']}`\n"
            f"{S.ACCOUNT} *الاسم:* {acc.get('full_name','—')}\n"
            f"{S.PHONE} *الهاتف:* `{acc['phone']}`\n"
            f"{S.LINK} *يوزر:* {'@'+acc['username'] if acc.get('username') else '—'}\n\n"
            f"`{'─'*22}`\n"
            f"*الحالة:* {badge}\n"
            f"*وقت الاتصال:* {ub.uptime() if ub else '—'}\n"
            f"*إعادة اتصال:* `{ub.reconnect_count if ub else 0}` مرة\n\n"
            f"`{'─'*22}`\n"
            f"{S.CHART} *إحصائيات:*\n"
            f"  رسائل مُرسلة: `{acc.get('msg_count',0)}`\n"
            f"  انضمامات: `{acc.get('join_count',0)}`\n"
            f"  أُضيف: {S.format_time(acc.get('created_at',''))}\n"
            f"  آخر نشاط: {S.format_time(acc.get('last_used','')) if acc.get('last_used') else '—'}"
        )
        edit_or_send(call, text, kb_account_actions(aid))
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_reconnect_"))
    def cb_acc_reconnect(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_")[-1])
        bot.answer_callback_query(call.id, "🔄 جاري إعادة الاتصال...")
        acc = arun(db_get_account(aid))
        if not acc:
            bot.send_message(call.message.chat.id, "❌ الحساب غير موجود")
            return
        # إيقاف القديم وإعادة تشغيله
        async def _reconnect():
            if aid in _clients:
                await _clients[aid].disconnect()
                _clients.pop(aid, None)
                _actions.pop(aid, None)
            return await _start_acc(acc)
        ok = arun(_reconnect())
        if ok:
            bot.send_message(call.message.chat.id, f"✅ *الحساب `{aid}` أُعيد اتصاله بنجاح!*", parse_mode="Markdown")
        else:
            bot.send_message(call.message.chat.id, f"❌ *فشل إعادة الاتصال للحساب `{aid}`*", parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_dialogs_"))
    def cb_acc_dialogs(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_")[-1])
        actions = mgr_actions(aid)
        if not actions:
            bot.answer_callback_query(call.id, "❌ الحساب غير متصل")
            return
        bot.answer_callback_query(call.id, "⏳ جاري التحميل...")
        dialogs = arun(actions.get_dialogs(20))
        if not dialogs:
            edit_or_send(call, "📭 _لا توجد محادثات_", kb_back(f"acc_detail_{aid}"))
            return
        lines = [f"{S.header(f'محادثات الحساب {aid}', S.GROUP)}\n"]
        for d in dialogs[:20]:
            name = getattr(d.entity,"title",None) or getattr(d.entity,"first_name","؟")
            eid  = d.entity.id
            uname = f"@{d.entity.username}" if getattr(d.entity,"username",None) else ""
            lines.append(f"• `{eid}` *{name}* {uname}")
        edit_or_send(call, "\n".join(lines), kb_back(f"acc_detail_{aid}"))

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_tasks_"))
    def cb_acc_tasks(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_")[-1])
        tasks = arun(db_get_all_tasks(account_id=aid))
        if not tasks:
            edit_or_send(call, f"📭 _لا توجد مهام للحساب {aid}_", kb_back(f"acc_detail_{aid}"))
            return
        lines = [f"{S.header(f'مهام الحساب {aid}', S.TASK)}\n"]
        for t in tasks:
            icon = S.task_type_icon(t["task_type"])
            lines.append(f"{icon} `{t['id']}` *{t['name']}* → `{t['target']}`")
        edit_or_send(call, "\n".join(lines), kb_back(f"acc_detail_{aid}"))

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_delete_"))
    def cb_acc_delete(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_")[-1])
        edit_or_send(
            call,
            f"⚠️ *هل تريد حذف الحساب `{aid}` نهائيًا؟*\n\n_سيتم حذف جميع مهامه وجداوله أيضًا!_",
            kb_confirm(f"acc_confirm_delete_{aid}", "acc_list")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_confirm_delete_"))
    def cb_acc_confirm_delete(call: CallbackQuery):
        aid = int(call.data.split("_")[-1])
        arun(mgr_remove(aid))
        edit_or_send(call, f"✅ *تم حذف الحساب `{aid}` بنجاح*", kb_back("acc_list"))
        bot.answer_callback_query(call.id, "✅ تم الحذف")

    @bot.callback_query_handler(func=lambda c: c.data == "acc_stats")
    def cb_acc_stats(call: CallbackQuery):
        if not is_admin(call): return
        statuses = mgr_all_status()
        if not statuses:
            edit_or_send(call, "📭 _لا توجد حسابات_", kb_back("menu_accounts"))
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header('حالة الحسابات', S.CHART)}\n"]
        for s in statuses:
            icon = "🟢" if s["connected"] else "🔴"
            bar  = S.progress_bar(1 if s["connected"] else 0, 1, 5)
            lines.append(
                f"\n{icon} *{s['name']}*\n"
                f"  ⏱ Uptime: `{s['uptime']}`\n"
                f"  🔄 Reconnects: `{s['reconnects']}`"
            )
        edit_or_send(call, "\n".join(lines), kb_back("menu_accounts"))
        bot.answer_callback_query(call.id)

    # ── إضافة حساب (wizard) ──

    @bot.callback_query_handler(func=lambda c: c.data == "acc_add")
    def cb_acc_add(call: CallbackQuery):
        if not is_admin(call): return
        uid = call.from_user.id
        set_state(uid, {"step":"acc_phone"})
        edit_or_send(call,
            f"{S.header('إضافة حساب جديد', S.NEW)}\n\n"
            f"{S.PHONE} أرسل رقم الهاتف بالصيغة الدولية:\n\n"
            f"مثال: `+201012345678`\n\n"
            f"_أو اضغط إلغاء_",
            kb_back("menu_accounts")
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(commands=["addaccount"])
    def cmd_addacc(msg: Message):
        if not is_admin(msg): return
        set_state(msg.from_user.id, {"step":"acc_phone"})
        bot.send_message(msg.chat.id,
            f"{S.header('إضافة حساب جديد', S.NEW)}\n\n"
            f"{S.PHONE} أرسل رقم الهاتف:\n`+201012345678`",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_phone"))
    def acc_step_phone(msg: Message):
        p = msg.text.strip()
        if not p.startswith("+") or not p[1:].replace(" ","").isdigit():
            return bot.reply_to(msg, "❌ صيغة خاطئة. مثال: `+201012345678`", parse_mode="Markdown")
        _states[msg.from_user.id]["phone"] = p
        _states[msg.from_user.id]["step"]  = "acc_api_id"
        bot.send_message(msg.chat.id,
            f"{S.header('API ID', S.KEY)}\n\n"
            f"روح *my.telegram.org* وأرسل الـ *API ID*\n\n"
            f"_رقم مكون من أرقام فقط_",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_api_id"))
    def acc_step_api_id(msg: Message):
        try:
            api_id = int(msg.text.strip())
        except ValueError:
            return bot.reply_to(msg, "❌ API ID يجب أن يكون رقمًا فقط")
        _states[msg.from_user.id]["api_id"] = api_id
        _states[msg.from_user.id]["step"]   = "acc_api_hash"
        bot.send_message(msg.chat.id,
            f"{S.header('API Hash', S.LOCK)}\n\n"
            f"أرسل الـ *API Hash*\n\n"
            f"_سلسلة حروف وأرقام_",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_api_hash"))
    def acc_step_api_hash(msg: Message):
        uid   = msg.from_user.id
        state = _states[uid]
        state["api_hash"] = msg.text.strip()
        state["step"]     = "acc_code"
        result = arun(mgr_begin_login(state["phone"], state["api_id"], state["api_hash"]))
        if result["status"] == "code_sent":
            bot.send_message(msg.chat.id,
                f"📨 *تم إرسال كود التحقق!*\n\n"
                f"{S.PHONE} إلى: `{state['phone']}`\n\n"
                f"أرسل الكود هنا 👇",
                parse_mode="Markdown"
            )
        else:
            clear_state(uid)
            bot.send_message(msg.chat.id, S.error(result.get("message","خطأ غير معروف")), parse_mode="Markdown")

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_code"))
    def acc_step_code(msg: Message):
        uid   = msg.from_user.id
        state = _states[uid]
        code  = msg.text.strip().replace(" ","").replace("-","")
        result = arun(mgr_complete_login(state["phone"], code))
        if result["status"] == "success":
            clear_state(uid)
            aid = result["account_id"]
            bot.send_message(msg.chat.id,
                f"`{'━'*28}`\n"
                f"  ✅ *تم تسجيل الدخول بنجاح!*\n"
                f"`{'━'*28}`\n\n"
                f"{S.ACCOUNT} *الاسم:* {result['name']}\n"
                f"{S.ID} *الـ ID:* `{aid}`\n"
                f"{S.LINK} *يوزر:* {'@'+result['username'] if result.get('username') else '—'}\n\n"
                f"🎉 _الحساب جاهز للاستخدام!_",
                parse_mode="Markdown",
                reply_markup=kb_back("menu_accounts")
            )
        elif result["status"] == "need_2fa":
            _states[uid]["step"] = "acc_2fa"
            bot.send_message(msg.chat.id,
                f"{S.LOCK} *التحقق الثنائي (2FA)*\n\nأرسل كلمة المرور:",
                parse_mode="Markdown"
            )
        else:
            clear_state(uid)
            bot.send_message(msg.chat.id, S.error(result.get("message","فشل تسجيل الدخول")), parse_mode="Markdown")

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_2fa"))
    def acc_step_2fa(msg: Message):
        uid   = msg.from_user.id
        state = _states[uid]
        phone = state["phone"]
        code  = state.get("code", "")
        result = arun(mgr_complete_login(phone, code, password=msg.text.strip()))
        clear_state(uid)
        if result["status"] == "success":
            bot.send_message(msg.chat.id,
                f"✅ *تم! مرحبًا {result['name']}*\n{S.ID} `{result['account_id']}`",
                parse_mode="Markdown", reply_markup=kb_back("menu_accounts")
            )
        else:
            bot.send_message(msg.chat.id, S.error(result.get("message","فشل 2FA")), parse_mode="Markdown")

    # ══════════════════════════════════════════
    #  ⚙️ المهام
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_tasks")
    def cb_menu_tasks(call: CallbackQuery):
        if not is_admin(call): return
        tasks = arun(db_get_all_tasks())
        text = (
            f"{S.header('إدارة المهام', S.TASK)}\n\n"
            f"{S.field('إجمالي المهام', len(tasks))}\n\n"
            f"اختر ما تريد 👇"
        )
        edit_or_send(call, text, kb_tasks_menu())
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "task_list")
    def cb_task_list(call: CallbackQuery):
        if not is_admin(call): return
        tasks = arun(db_get_all_tasks())
        if not tasks:
            text = (
                f"{S.header('المهام', S.TASK)}\n\n"
                f"📭 _لا توجد مهام_\n\nاضغط ➕ لإنشاء مهمة"
            )
            edit_or_send(call, text, kb_tasks_menu())
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header('قائمة المهام', S.TASK)}\n"]
        for t in tasks:
            icon  = S.task_type_icon(t["task_type"])
            tname = S.task_type_ar(t["task_type"])
            acc   = t.get("full_name") or t.get("phone","?")
            lines.append(
                f"\n{icon} `{t['id']}` *{t['name']}*\n"
                f"  📱 {acc} → `{t['target']}`\n"
                f"  🏷 {tname} • تُنفّذ: `{t.get('run_count',0)}` مرة"
            )
        kb = InlineKeyboardMarkup(row_width=2)
        for t in tasks[:8]:
            icon = S.task_type_icon(t["task_type"])
            kb.add(InlineKeyboardButton(
                f"{icon} {t['name'][:20]}",
                callback_data=f"task_detail_{t['id']}"
            ))
        kb.add(
            InlineKeyboardButton("➕ جديد",  callback_data="task_add"),
            InlineKeyboardButton("◀️ رجوع", callback_data="menu_tasks"),
        )
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_detail_"))
    def cb_task_detail(call: CallbackQuery):
        if not is_admin(call): return
        tid  = int(call.data.split("_")[-1])
        task = arun(db_get_task(tid))
        if not task:
            bot.answer_callback_query(call.id, "❌ المهمة غير موجودة")
            return
        acc = arun(db_get_account(task["account_id"]))
        icon = S.task_type_icon(task["task_type"])
        text = (
            f"{S.header('تفاصيل المهمة', icon)}\n\n"
            f"{S.ID} *ID:* `{task['id']}`\n"
            f"✏️ *الاسم:* {task['name']}\n"
            f"🏷 *النوع:* {S.task_type_ar(task['task_type'])}\n"
            f"🎯 *الهدف:* `{task['target']}`\n"
            f"📱 *الحساب:* {acc.get('full_name','?') if acc else '?'} (`{task['account_id']}`)\n\n"
            f"`{'─'*22}`\n"
        )
        if task.get("content"):
            preview = task["content"][:100] + ("..." if len(task["content"])>100 else "")
            text += f"📝 *المحتوى:*\n`{preview}`\n\n"
        text += (
            f"{S.CHART} *الإحصائيات:*\n"
            f"  تنفيذات: `{task.get('run_count',0)}`\n"
            f"  أُنشئت: {S.format_time(task.get('created_at',''))}"
        )
        edit_or_send(call, text, kb_task_actions(tid))
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_run_"))
    def cb_task_run(call: CallbackQuery):
        if not is_admin(call): return
        tid  = int(call.data.split("_")[-1])
        task = arun(db_get_task(tid))
        if not task:
            bot.answer_callback_query(call.id, "❌ المهمة غير موجودة")
            return
        actions = mgr_actions(task["account_id"])
        if not actions:
            bot.answer_callback_query(call.id, "❌ الحساب غير متصل")
            return
        bot.answer_callback_query(call.id, "▶️ جاري التنفيذ...")
        m = bot.send_message(call.message.chat.id, S.loading(f"تنفيذ المهمة {tid}"), parse_mode="Markdown")

        async def _run():
            t = task["task_type"]
            if t == "send_message": return await actions.send_message(task["target"], task["content"] or "", task["parse_mode"])
            elif t == "join_group":  return await actions.join_group(task["target"])
            elif t == "leave_group": return await actions.leave_group(task["target"])
            return False

        ok = arun(_run())
        result_text = (
            f"✅ *المهمة `{tid}` نُفّذت بنجاح!*\n"
            f"🎯 الهدف: `{task['target']}`"
        ) if ok else (
            f"❌ *فشل تنفيذ المهمة `{tid}`*\n"
            f"🎯 الهدف: `{task['target']}`"
        )
        try:
            bot.edit_message_text(result_text, m.chat.id, m.message_id, parse_mode="Markdown")
        except Exception:
            bot.send_message(call.message.chat.id, result_text, parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_delete_"))
    def cb_task_delete(call: CallbackQuery):
        if not is_admin(call): return
        tid = int(call.data.split("_")[-1])
        edit_or_send(
            call,
            f"⚠️ *حذف المهمة `{tid}`؟*\n\n_سيتم حذف جداولها أيضًا!_",
            kb_confirm(f"task_confirm_delete_{tid}", "task_list")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_confirm_delete_"))
    def cb_task_confirm_delete(call: CallbackQuery):
        tid = int(call.data.split("_")[-1])
        arun(db_delete_task(tid))
        edit_or_send(call, f"✅ *تم حذف المهمة `{tid}` بنجاح*", kb_back("task_list"))
        bot.answer_callback_query(call.id, "✅ تم الحذف")

    # ── إنشاء مهمة (wizard) ──

    @bot.callback_query_handler(func=lambda c: c.data == "task_add")
    def cb_task_add(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call, "❌ *لا توجد حسابات نشطة*\nأضف حسابًا أولًا أو تحقق من SESSION_ في متغيرات البيئة", kb_back("menu_accounts"))
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step":"task_account"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            icon = "🟢"
            kb.add(InlineKeyboardButton(
                f"{icon} {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"task_acc_pick_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"{S.header('مهمة جديدة - الخطوة 1/4', S.NEW)}\n\n"
            f"{S.ACCOUNT} اختر الحساب 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(commands=["addtask"])
    def cmd_addtask(msg: Message):
        if not is_admin(msg): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            return bot.reply_to(msg, "❌ لا توجد حسابات نشطة — تحقق من SESSION_ في متغيرات البيئة")
        uid = msg.from_user.id
        set_state(uid, {"step":"task_account"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            icon = "🟢"
            kb.add(InlineKeyboardButton(
                f"{icon} {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"task_acc_pick_{a['id']}"
            ))
        bot.send_message(msg.chat.id,
            f"{S.header('مهمة جديدة', S.NEW)}\n\n{S.ACCOUNT} اختر الحساب:",
            parse_mode="Markdown", reply_markup=kb
        )

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_acc_pick_"))
    def cb_task_acc_pick(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        aid = int(call.data.split("_")[-1])
        _states[uid]["account_id"] = aid
        _states[uid]["step"]       = "task_type"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("💬 إرسال رسالة",  callback_data="task_type_pick_send_message"),
            InlineKeyboardButton("👥 انضمام لجروب", callback_data="task_type_pick_join_group"),
            InlineKeyboardButton("🚪 مغادرة جروب",  callback_data="task_type_pick_leave_group"),
            InlineKeyboardButton("↪️ إعادة توجيه",  callback_data="task_type_pick_forward"),
            InlineKeyboardButton("❌ إلغاء",         callback_data="cancel"),
        )
        edit_or_send(call,
            f"{S.header('مهمة جديدة - الخطوة 2/4', S.TASK)}\n\n"
            f"⚙️ اختر *نوع* المهمة 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_type_pick_"))
    def cb_task_type_pick(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        ttype = call.data.replace("task_type_pick_","")
        _states[uid]["task_type"] = ttype
        _states[uid]["step"]      = "task_name"
        edit_or_send(call,
            f"{S.header('مهمة جديدة - الخطوة 3/4', S.EDIT)}\n\n"
            f"✏️ أرسل *اسمًا* للمهمة\n\n"
            f"_مثال: إرسال إعلان يومي_",
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"task_name"))
    def task_step_name(msg: Message):
        uid = msg.from_user.id
        _states[uid]["name"] = msg.text.strip()
        _states[uid]["step"] = "task_target"
        bot.send_message(msg.chat.id,
            f"{S.header('مهمة جديدة - الخطوة 4/4', S.PIN)}\n\n"
            f"🎯 أرسل *الهدف*\n\n"
            f"• Username: `@groupname`\n"
            f"• Chat ID: `-1001234567890`\n"
            f"• رابط دعوة: `t.me/+xxxxx`",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"task_target"))
    def task_step_target(msg: Message):
        uid   = msg.from_user.id
        state = _states[uid]
        state["target"] = msg.text.strip()
        if state["task_type"] in ("send_message","forward"):
            state["step"] = "task_content"
            prompt = (
                f"{S.header('محتوى الرسالة', S.SEND)}\n\n"
                f"📝 أرسل نص الرسالة\n\n"
                f"_يدعم Markdown: *غامق* `كود` __مائل___"
            ) if state["task_type"] == "send_message" else (
                f"{S.header('مصدر التوجيه', S.FORWARD)}\n\n"
                f"↪️ أرسل بالصيغة:\n`from_chat:message_id`\n\n"
                f"مثال: `@channel:123`"
            )
            bot.send_message(msg.chat.id, prompt, parse_mode="Markdown")
        else:
            _finish_task(msg, uid)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"task_content"))
    def task_step_content(msg: Message):
        uid = msg.from_user.id
        _states[uid]["content"] = msg.text
        _finish_task(msg, uid)

    def _finish_task(msg, uid):
        state = _states.get(uid, {})
        try:
            tid = arun(db_add_task(
                state["name"], state["account_id"], state["task_type"],
                state["target"], state.get("content"), created_by=uid
            ))
            icon = S.task_type_icon(state["task_type"])
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("▶️ تشغيل الآن",  callback_data=f"task_run_{tid}"),
                InlineKeyboardButton("📅 جدولة",        callback_data=f"task_sched_{tid}"),
                InlineKeyboardButton("📋 المهام",       callback_data="task_list"),
            )
            clear_state(uid)
            bot.send_message(msg.chat.id,
                f"`{'━'*28}`\n"
                f"  ✅ *المهمة أُنشئت بنجاح!*\n"
                f"`{'━'*28}`\n\n"
                f"{icon} *{state['name']}*\n"
                f"{S.ID} `{tid}`\n"
                f"🎯 `{state['target']}`\n"
                f"🏷 {S.task_type_ar(state['task_type'])}\n\n"
                f"اختر الخطوة التالية 👇",
                parse_mode="Markdown", reply_markup=kb
            )
        except Exception as e:
            bot.send_message(msg.chat.id, S.error(str(e)), parse_mode="Markdown")

    # ══════════════════════════════════════════
    #  📅 الجداول
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_schedules")
    def cb_menu_schedules(call: CallbackQuery):
        if not is_admin(call): return
        jobs  = sched_jobs()
        text = (
            f"{S.header('إدارة الجداول', S.SCHEDULE)}\n\n"
            f"{S.field('جداول نشطة', len(jobs), S.CLOCK)}\n"
            f"{S.field('المجدول', '🟢 يعمل' if _scheduler.running else '🔴 متوقف')}\n\n"
        )
        if jobs:
            text += "*أقرب مهمة:*\n"
            for j in jobs[:3]:
                text += f"  🕐 {j['name']} → `{j['next'][:16]}`\n"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("📋 عرض الكل",       callback_data="sched_list"),
            InlineKeyboardButton("➕ جدول جديد",      callback_data="sched_add_pick"),
            InlineKeyboardButton("◀️ رجوع",           callback_data="back_main"),
        )
        edit_or_send(call, text, kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "sched_list")
    def cb_sched_list(call: CallbackQuery):
        if not is_admin(call): return
        jobs = sched_jobs()
        if not jobs:
            edit_or_send(call,
                f"{S.header('الجداول', S.SCHEDULE)}\n\n📭 _لا توجد جداول نشطة_",
                kb_back("menu_schedules")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header('الجداول النشطة', S.CLOCK)}\n"]
        for j in jobs:
            lines.append(
                f"\n⏰ *{j['name']}*\n"
                f"  {S.ID} `{j['id']}`\n"
                f"  ⏭ التالي: `{str(j['next'])[:16]}`"
            )
        kb = InlineKeyboardMarkup(row_width=1)
        for j in jobs[:5]:
            kb.add(InlineKeyboardButton(
                f"🗑 حذف: {j['name'][:20]}",
                callback_data=f"sched_delete_{j['id'].lstrip('s')}"
            ))
        kb.add(InlineKeyboardButton("◀️ رجوع", callback_data="menu_schedules"))
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "sched_add_pick")
    def cb_sched_add_pick(call: CallbackQuery):
        if not is_admin(call): return
        tasks = arun(db_get_all_tasks())
        if not tasks:
            edit_or_send(call, "❌ *لا توجد مهام*\nأنشئ مهمة أولًا", kb_back("menu_tasks"))
            bot.answer_callback_query(call.id)
            return
        kb = InlineKeyboardMarkup(row_width=1)
        for t in tasks[:8]:
            icon = S.task_type_icon(t["task_type"])
            kb.add(InlineKeyboardButton(
                f"{icon} {t['name'][:25]} (#{t['id']})",
                callback_data=f"task_sched_{t['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"{S.header('جدولة مهمة', S.SCHEDULE)}\n\nاختر المهمة 👇", kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_sched_"))
    def cb_task_sched(call: CallbackQuery):
        uid = call.from_user.id
        tid = int(call.data.split("_")[-1])
        set_state(uid, {"step":"sched_type","task_id":tid})
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("⏰ مرة واحدة (Once)",         callback_data="sc_t_once"),
            InlineKeyboardButton("🔁 تكرار بفترة (Interval)",   callback_data="sc_t_interval"),
            InlineKeyboardButton("📆 جدول Cron",                callback_data="sc_t_cron"),
            InlineKeyboardButton("❌ إلغاء",                    callback_data="cancel"),
        )
        edit_or_send(call,
            f"{S.header(f'جدولة المهمة {tid}', S.CLOCK)}\n\n"
            f"اختر *نوع* الجدولة 👇\n\n"
            f"• *Once* = تشغيل مرة واحدة في وقت محدد\n"
            f"• *Interval* = تكرار كل فترة\n"
            f"• *Cron* = جدول متقدم (مثل Linux cron)",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data in ("sc_t_once","sc_t_interval","sc_t_cron"))
    def cb_sc_type(call: CallbackQuery):
        uid   = call.from_user.id
        state = get_state(uid)
        if not state: return
        stype = call.data.replace("sc_t_","")
        state["sched_type"] = stype
        state["step"]       = "sched_data"
        prompts = {
            "once": (
                f"{S.header('توقيت التشغيل', S.CLOCK)}\n\n"
                f"📅 أرسل التاريخ والوقت:\n\n"
                f"`YYYY-MM-DD HH:MM`\n\n"
                f"مثال: `2025-12-25 09:00`"
            ),
            "interval": (
                f"{S.header('فترة التكرار', S.REFRESH)}\n\n"
                f"🔁 أرسل الفترة الزمنية:\n\n"
                f"• `hours=2` كل ساعتين\n"
                f"• `minutes=30` كل 30 دقيقة\n"
                f"• `days=1` كل يوم\n"
                f"• `hours=1 minutes=30` كل ساعة ونصف"
            ),
            "cron": (
                f"{S.header('Cron Expression', S.GEAR)}\n\n"
                f"📆 أرسل تعبير Cron (5 حقول):\n\n"
                f"`دقيقة ساعة يوم شهر يوم_أسبوع`\n\n"
                f"أمثلة:\n"
                f"• `0 9 * * *` كل يوم 9 صباحًا\n"
                f"• `0 * * * *` كل ساعة\n"
                f"• `0 8 * * 1` كل اثنين 8ص\n"
                f"• `*/30 * * * *` كل 30 دقيقة"
            ),
        }
        edit_or_send(call, prompts[stype])
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"sched_data"))
    def sched_step_data(msg: Message):
        uid   = msg.from_user.id
        state = _states[uid]
        text  = msg.text.strip()
        stype = state["sched_type"]
        try:
            if stype == "once":
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M")
                if dt < datetime.now(TZ).replace(tzinfo=None):
                    return bot.reply_to(msg, "❌ الوقت في الماضي! أدخل وقتًا مستقبليًا")
                tdata = {"datetime": dt.isoformat()}
            elif stype == "interval":
                tdata = {}
                for part in text.split():
                    if "=" in part:
                        k, v = part.split("=",1)
                        k = k.strip().lower()
                        if k in ["weeks","days","hours","minutes","seconds"]:
                            tdata[k] = int(v.strip())
                if not tdata:
                    raise ValueError("لم يُتعرف على وحدة زمنية")
            elif stype == "cron":
                parts = text.split()
                if len(parts) != 5:
                    raise ValueError("يجب 5 حقول بالضبط")
                tdata = {"expression": text}
            else:
                raise ValueError("نوع غير معروف")
        except Exception as e:
            return bot.reply_to(msg, f"❌ {e}\n\nحاول مجددًا:")

        state["tdata"] = tdata
        state["step"]  = "sched_max"
        bot.send_message(msg.chat.id,
            f"{S.header('عدد مرات التشغيل', S.CHART)}\n\n"
            f"🔢 كم مرة تريد تشغيل هذه المهمة؟\n\n"
            f"• أدخل رقمًا مثل `5`\n"
            f"• أو `0` للتشغيل *اللانهائي* ∞",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"sched_max"))
    def sched_step_max(msg: Message):
        uid   = msg.from_user.id
        state = _states.get(uid, {})
        try:
            n = int(msg.text.strip())
            if n < 0: raise ValueError()
        except ValueError:
            return bot.reply_to(msg, "❌ أدخل رقمًا صحيحًا (0 = لانهائي)")

        max_runs = -1 if n == 0 else n
        tid   = state["task_id"]
        stype = state["sched_type"]
        tdata = state["tdata"]
        try:
            sid = arun(sched_add(tid, stype, tdata, max_runs))
            runs_txt = "♾️ لانهائي" if n == 0 else f"{n} مرة"
            bot.send_message(msg.chat.id,
                f"`{'━'*28}`\n"
                f"  📅 *تم إنشاء الجدول بنجاح!*\n"
                f"`{'━'*28}`\n\n"
                f"{S.ID} `{sid}`\n"
                f"⚙️ المهمة: `{tid}`\n"
                f"🏷 النوع: {S.trigger_type_ar(stype)}\n"
                f"🔢 المرات: {runs_txt}\n\n"
                f"🚀 _الجدول يعمل الآن!_",
                parse_mode="Markdown",
                reply_markup=kb_back("menu_schedules")
            )
        except Exception as e:
            bot.send_message(msg.chat.id, S.error(str(e)), parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("sched_delete_"))
    def cb_sched_delete(call: CallbackQuery):
        if not is_admin(call): return
        sid_str = call.data.replace("sched_delete_","")
        try: sid = int(sid_str)
        except ValueError: sid = 0
        edit_or_send(call,
            f"⚠️ *حذف الجدول `{sid}`؟*",
            kb_confirm(f"sched_confirm_{sid}", "sched_list")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("sched_confirm_"))
    def cb_sched_confirm(call: CallbackQuery):
        sid = int(call.data.split("_")[-1])
        arun(sched_remove(sid))
        edit_or_send(call, f"✅ *تم حذف الجدول `{sid}`*", kb_back("menu_schedules"))
        bot.answer_callback_query(call.id, "✅ تم")

    # ══════════════════════════════════════════
    #  📊 الإحصائيات
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_stats")
    def cb_menu_stats(call: CallbackQuery):
        if not is_admin(call): return
        bot.answer_callback_query(call.id, "⏳ جاري التحميل...")
        stats = arun(db_get_stats())
        connected = mgr_count()
        total_acc = stats["total_accounts"]
        success_rate = (
            round(stats["ok_total"] / max(stats["total_logs"],1) * 100, 1)
        ) if stats["total_logs"] > 0 else 0

        bar_today = S.progress_bar(stats["ok_today"], max(stats["ok_today"]+stats["fail_today"],1))
        bar_total = S.progress_bar(stats["ok_total"], max(stats["total_logs"],1))

        text = (
            f"`{'━'*28}`\n"
            f"  📊 *إحصائيات النظام*\n"
            f"`{'━'*28}`\n\n"

            f"👤 *الحسابات:*\n"
            f"  الإجمالي:  `{total_acc}`\n"
            f"  متصل:      `{connected}` 🟢\n"
            f"  محظور:     `{stats['banned_accounts']}` 🚫\n\n"

            f"⚙️ *المهام والجداول:*\n"
            f"  المهام:    `{stats['total_tasks']}`\n"
            f"  الجداول:   `{stats['total_schedules']}`\n\n"

            f"📈 *الأداء اليوم:*\n"
            f"  ✅ نجح: `{stats['ok_today']}`  ❌ فشل: `{stats['fail_today']}`\n"
            f"  `{bar_today}`\n\n"

            f"📊 *الأداء الكلي:*\n"
            f"  ✅ نجح: `{stats['ok_total']}`  ❌ فشل: `{stats['fail_total']}`\n"
            f"  `{bar_total}`\n"
            f"  معدل النجاح: `{success_rate}%`\n\n"

            f"💬 *إجمالي الرسائل المُرسلة:* `{S.format_number(stats['total_msgs'])}`"
        )
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🔄 تحديث",  callback_data="menu_stats"),
            InlineKeyboardButton("◀️ رجوع",   callback_data="back_main"),
        )
        edit_or_send(call, text, kb)

    # ══════════════════════════════════════════
    #  📋 السجلات
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_logs")
    def cb_menu_logs(call: CallbackQuery):
        if not is_admin(call): return
        logs = arun(db_get_logs(15))
        if not logs:
            edit_or_send(call,
                f"{S.header('السجلات', S.LOG)}\n\n📭 _لا توجد سجلات بعد_",
                kb_back("back_main")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header('آخر العمليات', S.LOG)}\n"]
        for l in logs:
            icon  = "✅" if l["status"]=="success" else "❌"
            tname = l.get("task_name","؟")
            phone = l.get("phone","")
            dur   = f" `{l.get('duration_ms',0)}ms`" if l.get("duration_ms") else ""
            lines.append(
                f"{icon} `{str(l['executed_at'])[:16]}` "
                f"*{tname}*{dur}\n"
                f"  📱 {phone}"
            )
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ الناجحة فقط",  callback_data="logs_success"),
            InlineKeyboardButton("❌ الفاشلة فقط",  callback_data="logs_failed"),
            InlineKeyboardButton("🔄 تحديث",        callback_data="menu_logs"),
            InlineKeyboardButton("◀️ رجوع",         callback_data="back_main"),
        )
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data in ("logs_success","logs_failed"))
    def cb_logs_filter(call: CallbackQuery):
        status = "success" if call.data == "logs_success" else "failed"
        logs   = arun(db_get_logs(15, status=status))
        icon   = "✅" if status=="success" else "❌"
        if not logs:
            edit_or_send(call, f"📭 _لا توجد عمليات {icon}_", kb_back("menu_logs"))
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header(f'السجلات {icon}', S.LOG)}\n"]
        for l in logs:
            lines.append(
                f"{icon} `{str(l['executed_at'])[:16]}` *{l.get('task_name','؟')}*\n"
                f"  {l.get('message','') or '—'}"
            )
        edit_or_send(call, "\n".join(lines), kb_back("menu_logs"))
        bot.answer_callback_query(call.id)

    # ══════════════════════════════════════════
    #  🛡 الحماية
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_protection")
    def cb_menu_protection(call: CallbackQuery):
        if not is_admin(call): return
        bl = arun(db_get_blacklist())
        text = (
            f"{S.header('إعدادات الحماية', S.SHIELD)}\n\n"
            f"🚫 *القائمة السوداء:* `{len(bl)}` هدف\n\n"
            f"*إعدادات Anti-Ban:*\n"
            f"  تأخير الإرسال: `{MIN_DELAY}-{MAX_DELAY}s`\n"
            f"  تأخير الانضمام: `{JOIN_DELAY}s`\n"
            f"  حد الإرسال: `{RATE_MSGS}` رسالة / `{RATE_PERIOD}s`\n\n"
        )
        if bl:
            text += "*القائمة السوداء:*\n"
            for b in bl[:5]:
                text += f"  🚫 `{b['target']}` — {b.get('reason','—')}\n"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🚫 إضافة للقائمة السوداء", callback_data="bl_add"),
            InlineKeyboardButton("✅ إزالة من القائمة",       callback_data="bl_remove"),
            InlineKeyboardButton("📋 عرض القائمة",           callback_data="bl_list"),
            InlineKeyboardButton("◀️ رجوع",                   callback_data="back_main"),
        )
        edit_or_send(call, text, kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "bl_add")
    def cb_bl_add(call: CallbackQuery):
        if not is_admin(call): return
        uid = call.from_user.id
        set_state(uid, {"step":"bl_add_target"})
        edit_or_send(call,
            f"{S.header('إضافة للقائمة السوداء', S.BANNED)}\n\n"
            f"أرسل اسم المستخدم أو الـ ID:\n`@username` أو `-1001234`"
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"bl_add_target"))
    def bl_step_target(msg: Message):
        uid    = msg.from_user.id
        target = msg.text.strip()
        clear_state(uid)
        arun(db_add_blacklist(target, "أُضيف يدويًا"))
        bot.send_message(msg.chat.id,
            f"✅ *تم إضافة* `{target}` *للقائمة السوداء*",
            parse_mode="Markdown",
            reply_markup=kb_back("menu_protection")
        )

    @bot.callback_query_handler(func=lambda c: c.data == "bl_list")
    def cb_bl_list(call: CallbackQuery):
        if not is_admin(call): return
        bl = arun(db_get_blacklist())
        if not bl:
            edit_or_send(call, "📭 _القائمة السوداء فارغة_", kb_back("menu_protection"))
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header('القائمة السوداء', S.BANNED)}\n"]
        for b in bl:
            lines.append(f"🚫 `{b['target']}` — _{b.get('reason','—')}_")
        edit_or_send(call, "\n".join(lines), kb_back("menu_protection"))
        bot.answer_callback_query(call.id)

    # ══════════════════════════════════════════
    #  🔔 الإشعارات
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_notifications")
    def cb_notifications(call: CallbackQuery):
        if not is_admin(call): return
        notifs = arun(db_get_unread_notifications())
        if not notifs:
            edit_or_send(call,
                f"{S.header('الإشعارات', S.BELL)}\n\n✅ _لا توجد إشعارات جديدة_",
                kb_back("back_main")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [f"{S.header(f'إشعارات جديدة ({len(notifs)})', S.BELL)}\n"]
        icons = {"reconnect":"🔄","ban":"🚫","new_account":"🆕","error":"❌"}
        for n in notifs:
            icon = icons.get(n["type"],"🔔")
            lines.append(
                f"{icon} *{n['title']}*\n"
                f"  _{n.get('body','')}_\n"
                f"  🕐 {S.format_time(n['created_at'])}"
            )
        arun(db_mark_notifications_read())
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("◀️ رجوع", callback_data="back_main"))
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    # ══════════════════════════════════════════
    #  ⌨️ أوامر نصية سريعة
    # ══════════════════════════════════════════

    @bot.message_handler(commands=["ping"])
    def cmd_ping(msg: Message):
        if not is_admin(msg): return
        connected = mgr_count()
        jobs      = len(sched_jobs())
        bot.reply_to(msg,
            f"`{'━'*26}`\n"
            f"  🏓 *Pong!*\n"
            f"`{'━'*26}`\n\n"
            f"🟢 متصل: `{connected}`\n"
            f"📅 جداول: `{jobs}`\n"
            f"🤖 المجدول: {'🟢 يعمل' if _scheduler.running else '🔴 متوقف'}",
            parse_mode="Markdown"
        )

    @bot.message_handler(commands=["stats"])
    def cmd_stats(msg: Message):
        if not is_admin(msg): return
        stats = arun(db_get_stats())
        bot.send_message(msg.chat.id,
            f"{S.header('الإحصائيات', S.CHART)}\n\n"
            f"👤 حسابات: `{stats['active_accounts']}/{stats['total_accounts']}`\n"
            f"🟢 متصل: `{mgr_count()}`\n"
            f"⚙️ مهام: `{stats['total_tasks']}`\n"
            f"📅 جداول: `{stats['total_schedules']}`\n\n"
            f"اليوم: ✅`{stats['ok_today']}` ❌`{stats['fail_today']}`\n"
            f"الكل:  ✅`{stats['ok_total']}` ❌`{stats['fail_total']}`",
            parse_mode="Markdown"
        )

    @bot.message_handler(commands=["send"])
    def cmd_send(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split(maxsplit=3)
        if len(parts) < 4:
            return bot.reply_to(msg,
                "الاستخدام:\n`/send <acc_id> <target> <رسالة>`\n\n"
                "مثال:\n`/send 1 @group مرحبا!`",
                parse_mode="Markdown"
            )
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id غير صحيح", parse_mode="Markdown")
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, f"❌ الحساب `{aid}` غير متصل", parse_mode="Markdown")
        m = bot.reply_to(msg, S.loading(f"إرسال إلى {parts[2]}"), parse_mode="Markdown")
        ok = arun(actions.send_message(parts[2], parts[3]))
        bot.edit_message_text(
            f"✅ *تم الإرسال إلى* `{parts[2]}`" if ok else f"❌ *فشل الإرسال إلى* `{parts[2]}`",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["broadcast"])
    def cmd_broadcast(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split(maxsplit=2)
        if len(parts) < 3:
            return bot.reply_to(msg,
                "الاستخدام:\n`/broadcast <target> <رسالة>`\n\n_يُرسل من جميع الحسابات_",
                parse_mode="Markdown"
            )
        target, text = parts[1], parts[2]
        ids = mgr_all_connected()
        if not ids: return bot.reply_to(msg, "❌ لا توجد حسابات متصلة")
        m = bot.send_message(msg.chat.id,
            f"📡 *بث جماعي*\n🎯 `{target}`\n👤 {len(ids)} حساب\n\n{S.loading('جاري الإرسال')}",
            parse_mode="Markdown"
        )
        ok = fail = 0
        for aid in ids:
            actions = mgr_actions(aid)
            if actions:
                r = arun(actions.send_message(target, text))
                if r: ok += 1
                else: fail += 1
        bar = S.progress_bar(ok, ok+fail)
        bot.edit_message_text(
            f"📡 *نتيجة البث*\n\n"
            f"🎯 `{target}`\n"
            f"✅ نجح: `{ok}` | ❌ فشل: `{fail}`\n"
            f"`{bar}`",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["join"])
    def cmd_join(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 3:
            return bot.reply_to(msg, "الاستخدام: `/join <acc_id> <target>`", parse_mode="Markdown")
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id غير صحيح", parse_mode="Markdown")
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, "❌ الحساب غير متصل")
        m = bot.reply_to(msg, S.loading(f"انضمام إلى {parts[2]}"), parse_mode="Markdown")
        ok = arun(actions.join_group(parts[2]))
        bot.edit_message_text(
            f"✅ *انضم إلى* `{parts[2]}`" if ok else f"❌ *فشل الانضمام إلى* `{parts[2]}`",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["dialogs"])
    def cmd_dialogs(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2:
            return bot.reply_to(msg, "الاستخدام: `/dialogs <acc_id>`", parse_mode="Markdown")
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id غير صحيح", parse_mode="Markdown")
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, "❌ الحساب غير متصل")
        m = bot.reply_to(msg, S.loading("تحميل المحادثات"), parse_mode="Markdown")
        dialogs = arun(actions.get_dialogs(20))
        if not dialogs:
            bot.edit_message_text("📭 لا توجد محادثات", m.chat.id, m.message_id)
            return
        lines = [f"{S.header(f'محادثات الحساب {aid}', S.GROUP)}\n"]
        for d in dialogs:
            name  = getattr(d.entity,"title",None) or getattr(d.entity,"first_name","؟")
            uname = f"@{d.entity.username}" if getattr(d.entity,"username",None) else ""
            lines.append(f"• `{d.entity.id}` *{name}* {uname}")
        bot.edit_message_text("\n".join(lines), m.chat.id, m.message_id, parse_mode="Markdown")

    @bot.message_handler(commands=["runtask"])
    def cmd_runtask(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2: return bot.reply_to(msg, "الاستخدام: `/runtask <id>`", parse_mode="Markdown")
        try: tid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ ID غير صحيح")
        task = arun(db_get_task(tid))
        if not task: return bot.reply_to(msg, "❌ المهمة غير موجودة")
        actions = mgr_actions(task["account_id"])
        if not actions: return bot.reply_to(msg, "❌ الحساب غير متصل")
        m = bot.reply_to(msg, S.loading(f"تنفيذ المهمة {tid}"), parse_mode="Markdown")
        async def _r():
            t = task["task_type"]
            if t=="send_message": return await actions.send_message(task["target"],task["content"] or "",task["parse_mode"])
            elif t=="join_group":  return await actions.join_group(task["target"])
            elif t=="leave_group": return await actions.leave_group(task["target"])
            return False
        ok = arun(_r())
        bot.edit_message_text(
            f"✅ *المهمة `{tid}` نُفّذت!*" if ok else f"❌ *فشلت المهمة `{tid}`*",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["delaccount"])
    def cmd_delacc(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2: return bot.reply_to(msg, "الاستخدام: `/delaccount <id>`", parse_mode="Markdown")
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ ID غير صحيح")
        arun(mgr_remove(aid))
        bot.reply_to(msg, f"✅ *تم حذف الحساب* `{aid}`", parse_mode="Markdown")

    @bot.message_handler(commands=["deltask"])
    def cmd_deltask(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2: return bot.reply_to(msg, "الاستخدام: `/deltask <id>`", parse_mode="Markdown")
        try: tid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ ID غير صحيح")
        arun(db_delete_task(tid))
        bot.reply_to(msg, f"✅ *تم حذف المهمة* `{tid}`", parse_mode="Markdown")

    @bot.message_handler(commands=["help"])
    def cmd_help(msg: Message):
        if not is_admin(msg): return
        bot.send_message(msg.chat.id,
            f"`{'━'*28}`\n"
            f"  {S.BOT} *دليل الأوامر*\n"
            f"`{'━'*28}`\n\n"
            f"*👤 الحسابات:*\n"
            f"/addaccount — إضافة حساب جديد\n"
            f"/delaccount `<id>` — حذف حساب\n\n"
            f"*⚙️ المهام:*\n"
            f"/addtask — إنشاء مهمة جديدة\n"
            f"/runtask `<id>` — تشغيل فوري\n"
            f"/deltask `<id>` — حذف مهمة\n\n"
            f"*📡 إرسال سريع:*\n"
            f"/send `<acc>` `<target>` `<msg>`\n"
            f"/broadcast `<target>` `<msg>`\n"
            f"/join `<acc>` `<target>`\n"
            f"/dialogs `<acc>` — محادثات\n\n"
            f"*📊 نظام:*\n"
            f"/stats — إحصائيات\n"
            f"/ping — اختبار\n"
            f"/menu — القائمة الرئيسية\n\n"
            f"*🔑 Session Strings:*\n"
            f"/showsessions — عرض جلسات البيئة\n"
            f"/reloadsessions — إعادة تحميل الجلسات",
            parse_mode="Markdown",
            reply_markup=kb_main_menu()
        )

    @bot.message_handler(commands=["setcommands"])
    def cmd_setcommands(msg: Message):
        if not is_admin(msg): return
        bot.set_my_commands([
            BotCommand("start",       "🏠 القائمة الرئيسية"),
            BotCommand("menu",        "🎛 لوحة التحكم"),
            BotCommand("stats",       "📊 الإحصائيات"),
            BotCommand("ping",        "🏓 اختبار الاتصال"),
            BotCommand("addaccount",  "👤 إضافة حساب"),
            BotCommand("addtask",     "⚙️ إضافة مهمة"),
            BotCommand("runtask",     "▶️ تشغيل مهمة"),
            BotCommand("send",        "📤 إرسال رسالة"),
            BotCommand("broadcast",   "📡 بث جماعي"),
            BotCommand("join",        "👥 انضمام لجروب"),
            BotCommand("dialogs",        "💬 عرض المحادثات"),
            BotCommand("showsessions",   "🔑 عرض Session Strings"),
            BotCommand("reloadsessions", "🔄 إعادة تحميل الجلسات"),
            BotCommand("help",           "❓ المساعدة"),
        ])
        bot.reply_to(msg, "✅ *تم تعيين الأوامر بنجاح!*", parse_mode="Markdown")

    # ── Fallback ──
    @bot.message_handler(commands=["showsessions"])
    def cmd_showsessions(msg: Message):
        if not is_admin(msg): return
        if not SESSION_ACCOUNTS:
            return bot.reply_to(msg,
                "⚠️ *لا توجد Session Strings في متغيرات البيئة*\n\n"
                "_أضف متغيرات بالشكل: `SESSION_1`, `SESSION_2`, ..._",
                parse_mode="Markdown"
            )
        lines = [f"🔑 *Session Strings المحملة*\n`{'━'*28}`\n"]
        for num, ss in SESSION_ACCOUNTS.items():
            fake_id = -(abs(hash(ss)) % 999999 + 1)
            client = _clients.get(fake_id)
            status = S.status_badge(bool(client.is_connected) if client else False)
            name = client.display_name() if client else "—"
            preview = ss[:8] + "..." + ss[-4:] if len(ss) > 14 else ss
            lines.append(
                f"▸ *SESSION\\_{num}*\n"
                f"  👤 {name} | {status}\n"
                f"  🆔 `{fake_id}`\n"
                f"  🔐 `{preview}`\n"
            )
        bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="Markdown")

    @bot.message_handler(commands=["reloadsessions"])
    def cmd_reloadsessions(msg: Message):
        if not is_admin(msg): return
        if not SESSION_ACCOUNTS:
            return bot.reply_to(msg, "⚠️ لا توجد SESSION_ متغيرات في البيئة", parse_mode="Markdown")
        sent_msg = bot.reply_to(msg,
            f"🔄 *إعادة تحميل {len(SESSION_ACCOUNTS)} Session String...*",
            parse_mode="Markdown"
        )
        # حذف الجلسات البيئية القديمة أولاً
        to_remove = [aid for aid in list(_clients.keys()) if aid < 0]
        for aid in to_remove:
            arun(_clients[aid].disconnect())
            _clients.pop(aid, None)
            _actions.pop(aid, None)
        # إعادة تحميل من متغيرات البيئة الحالية
        fresh = load_all_sessions()
        SESSION_ACCOUNTS.clear()
        SESSION_ACCOUNTS.update(fresh)
        results = arun(asyncio.gather(
            *[load_session_from_env(num, ss) for num, ss in SESSION_ACCOUNTS.items()],
            return_exceptions=True
        ))
        ok = sum(1 for r in results if r is True)
        bot.edit_message_text(
            f"✅ *إعادة التحميل اكتملت*\n\n"
            f"🔑 إجمالي: `{len(SESSION_ACCOUNTS)}`\n"
            f"🟢 متصل: `{ok}`\n"
            f"🔴 فشل: `{len(SESSION_ACCOUNTS) - ok}`",
            sent_msg.chat.id, sent_msg.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: True)
    def fallback(msg: Message):
        if not is_admin(msg): return
        # تجاهل الرسائل غير المعروفة

    logger.info("✅ البوت جاهز مع جميع الأوامر")

# ══════════════════════════════════════════════
#  🚀 Main
# ══════════════════════════════════════════════

async def startup():
    logger.info("╔══════════════════════════════════╗")
    logger.info(f"║  🤖 {BOT_NAME} v{VERSION}".ljust(34) + "║")
    logger.info("╚══════════════════════════════════╝")
    await db_connect()
    await mgr_load_all()
    await sched_load_all()
    _scheduler.start()
    logger.info("✅ النظام يعمل بالكامل!")

async def _send_startup_message():
    """إرسال رسالة بدء التشغيل لجميع الأدمنز"""
    if not bot or not ADMIN_IDS:
        return
    connected = mgr_get_all_for_task()
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    divider = "\u2501" * 28
    if connected:
        lines = []
        for a in connected:
            tag = " \U0001f511" if a["is_env"] else ""
            lines.append(f"  \U0001f7e2 {a['full_name']} (#{a['id']}){tag}")
        acc_block = "\n".join(lines)
        text = (
            "\U0001f916 *" + BOT_NAME + " \u0634\u063a\u0627\u0644!*\n"
            "`" + divider + "`\n\n"
            "\U0001f550 `" + now + "`\n"
            "\u2705 *" + str(len(connected)) + " \u062d\u0633\u0627\u0628 \u0645\u062a\u0635\u0644:*\n"
            + acc_block
        )
    else:
        text = (
            "\U0001f916 *" + BOT_NAME + " \u0634\u063a\u0627\u0644!*\n"
            "`" + divider + "`\n\n"
            "\U0001f550 `" + now + "`\n"
            "\u26a0\ufe0f \u0644\u0627 \u062a\u0648\u062c\u062f \u062d\u0633\u0627\u0628\u0627\u062a \u0645\u062a\u0635\u0644\u0629 \u062d\u0627\u0644\u064a\u0627\u064b"
        )
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"startup msg failed for {admin_id}: {e}")


async def shutdown():
    logger.info("⏳ جاري الإيقاف...")
    try: _scheduler.shutdown(wait=False)
    except Exception: pass
    await mgr_stop_all()
    await db_close()
    logger.info("✅ تم الإيقاف بنجاح")

def run_bot():
    if not bot: return
    logger.info("🤖 بوت التحكم بدأ...")
    bot.infinity_polling(
        timeout=30, long_polling_timeout=30,
        logger_level=logging.WARNING,
        allowed_updates=["message","callback_query"],
    )

def main():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    try:
        _loop.run_until_complete(startup())
    except Exception as e:
        logger.critical(f"فشل بدء التشغيل: {e}")
        sys.exit(1)
    if not BOT_TOKEN:
        logger.warning("⚠️ BOT_TOKEN غير موجود!")
    else:
        setup_bot()
        t = threading.Thread(target=run_bot, daemon=True)
        t.start()
        # إرسال رسالة بدء التشغيل للأدمن
        import time as _time
        _time.sleep(2)  # انتظر حتى يتصل البوت
        _loop.run_until_complete(_send_startup_message())

    def _sig(sig, frame):
        _loop.run_until_complete(shutdown())
        _loop.close()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)
    logger.info("✅ شغّال! اضغط Ctrl+C للإيقاف")
    try:
        _loop.run_forever()
    except KeyboardInterrupt:
        _loop.run_until_complete(shutdown())
    finally:
        _loop.close()

if __name__ == "__main__":
    main()
