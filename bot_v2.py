"""
╔══════════════════════════════════════════════════════════════╗
║  ⚡ TG MANAGER PRO v3.0  —  Multi-Account Telegram Bot      ║
║  ▸ Full Inline UI   ▸ Session Strings   ▸ Anti-Ban          ║
║  ▸ Smart Scheduler  ▸ Broadcast  ▸ DM Friends               ║
╚══════════════════════════════════════════════════════════════╝

المتطلبات:
pip install telethon pyTelegramBotAPI apscheduler aiosqlite cryptography pytz python-dotenv
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
    """تنسيق موحد لجميع رسائل البوت — v3 Dark Theme"""

    HEADER   = "▰" * 14
    DIVIDER  = "┄" * 20
    LINE     = "▱" * 12

    SUCCESS   = "✅"; ERROR    = "❌"; WARNING  = "⚠️"; INFO     = "ℹ️"
    LOADING   = "⏳"; DONE    = "🎯"; FIRE     = "🔥"; STAR     = "⭐"
    LOCK      = "🔐"; PHONE   = "📱"; BOT      = "🤖"; ACCOUNT  = "👤"
    TASK      = "⚙️"; SCHEDULE= "📅"; STATS    = "📊"; LOG      = "📋"
    SEND      = "📤"; RECEIVE = "📥"; GROUP    = "👥"; SHIELD   = "🛡️"
    CLOCK     = "⏰"; ROCKET  = "🚀"; GEAR     = "⚙️"; DATABASE = "🗄️"
    ONLINE    = "🟢"; OFFLINE = "🔴"; IDLE     = "🟡"; BANNED   = "🚫"
    CROWN     = "👑"; LIGHTNING="⚡"; CHART    = "📈"; BELL     = "🔔"
    PIN       = "📌"; FOLDER  = "📁"; KEY      = "🔑"; LINK     = "🔗"
    SEARCH    = "🔍"; TRASH   = "🗑️"; EDIT     = "✏️"; BACK     = "◀️"
    NEXT      = "▶️"; UP      = "⬆️"; DOWN     = "⬇️"; REFRESH  = "🔄"
    PLAY      = "▶️"; STOP    = "⏹️"; PAUSE    = "⏸️"; FORWARD  = "↪️"
    NEW       = "🆕"; ID      = "🆔"; BROADCAST= "📡"; WORLD    = "🌍"
    COPY      = "📋"; DIAMOND = "💎"; ALIEN    = "👾"; SPARKLE  = "✨"
    FRIEND    = "🤝"; CHAT    = "💬"; LEAVE    = "🚪"; JOIN     = "🔗"

    @staticmethod
    def header(title: str, icon: str = "⚡") -> str:
        top = "╔" + "═" * 26 + "╗"
        mid = f"║  {icon}  {title}"
        mid = mid.ljust(28) + "║"
        bot_ = "╚" + "═" * 26 + "╝"
        return f"`{top}`\n`{mid}`\n`{bot_}`"

    @staticmethod
    def mini_header(title: str, icon: str = "▸") -> str:
        return f"┌─ {icon} *{title}*\n└{'─'*18}"

    @staticmethod
    def section(title: str, icon: str = "◆") -> str:
        return f"\n{icon} *{title}*"

    @staticmethod
    def field(label: str, value, icon: str = "▸") -> str:
        return f"  {icon} {label}: `{value}`"

    @staticmethod
    def success(msg: str) -> str:
        return f"✅ *{msg}*"

    @staticmethod
    def error(msg: str) -> str:
        return f"❌ *خطأ:* _{msg}_"

    @staticmethod
    def warning(msg: str) -> str:
        return f"⚠️ *تحذير:* _{msg}_"

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
        if is_banned:    return "🚫 *محظور*"
        if is_connected: return "🟢 *متصل*"
        return "🔴 *منفصل*"

    @staticmethod
    def progress_bar(current: int, total: int, length: int = 12) -> str:
        if total == 0: return "░" * length
        filled = int(length * current / total)
        return "▓" * filled + "░" * (length - filled)

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
            if total < 60:    return "🕐 منذ لحظات"
            if total < 3600:  return f"🕐 منذ {total//60} دقيقة"
            if total < 86400: return f"🕐 منذ {total//3600} ساعة"
            if diff.days == 1:       return "📅 أمس"
            return f"📅 {dt.strftime('%Y-%m-%d %H:%M')}"
        except Exception:
            return dt_str[:16] if dt_str else "—"

    @staticmethod
    def task_type_icon(task_type: str) -> str:
        return {
            "send_message": "💬",
            "join_group":   "🔗",
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
            "once":     "⏰ مرة واحدة",
            "interval": "🔁 تكرار",
            "cron":     "📆 Cron",
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
MIN_DELAY   = float(os.getenv("MIN_DELAY", "0.5"))
MAX_DELAY   = float(os.getenv("MAX_DELAY", "2.0"))
JOIN_DELAY  = float(os.getenv("JOIN_DELAY", "30.0"))
RATE_MSGS   = int(os.getenv("RATE_LIMIT_MESSAGES", "20"))
RATE_PERIOD = int(os.getenv("RATE_LIMIT_PERIOD", "60"))
BOT_NAME    = os.getenv("BOT_NAME", "TG Manager Pro")
VERSION     = "4.0.0"

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

_ALLOWED_STAT_FIELDS = {"msg_count", "join_count"}

async def db_increment_account_stat(aid: int, field: str):
    if field not in _ALLOWED_STAT_FIELDS:
        logger.error(f"حقل غير مسموح: {field}")
        return
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    await _db.execute(
        f"UPDATE accounts SET {field}={field}+1, last_used=? WHERE id=?",
        (now_iso, aid)
    )
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
            "SELECT t.*, a.phone, a.username, a.full_name FROM tasks t LEFT JOIN accounts a ON t.account_id=a.id WHERE t.is_active=1 AND t.account_id=? ORDER BY t.id DESC",
            (account_id,)
        )
    else:
        cur = await _db.execute(
            "SELECT t.*, a.phone, a.username, a.full_name FROM tasks t LEFT JOIN accounts a ON t.account_id=a.id WHERE t.is_active=1 ORDER BY t.id DESC"
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
        while True:
            async with self._lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            # حرر القفل أثناء الانتظار حتى لا يُعطّل المهام الأخرى
            await asyncio.sleep(wait)

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

    # Jitter إضافي أحيانًا (مخفف)
    if random.random() < 0.05:
        delay += random.uniform(0.5, 2)

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
        try:
            r = await self._client.send_code_request(self.phone)
            return r.phone_code_hash
        except FloodWaitError as e:
            logger.warning(f"[{self.phone}] FloodWait عند طلب الكود: {e.seconds}s")
            raise Exception(f"⚠️ انتظر {e.seconds} ثانية ثم حاول مجدداً (Flood)")
        except Exception:
            raise

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

    async def send_message(self, target, text, parse_mode="markdown", typing=False, reply_to=None) -> bool:
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
            try:
                ok = await self.send_message(t, text, parse_mode)
                results[str(t)] = "success" if ok else "failed"
            except Exception as e:
                logger.error(f"[{self.ub.phone}] send_to_many فشل لـ {t}: {e}")
                results[str(t)] = "failed"
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
        """مغادرة جروب — يدعم username, chat_id, رابط دعوة"""
        try:
            # تنظيف الرابط إذا كان رابط دعوة
            if isinstance(target, str) and ("t.me/+" in target or "t.me/joinchat/" in target):
                # محاولة الحصول على entity من الرابط
                slug = target.split("/")[-1].replace("+", "")
                try:
                    entity = await self.ub.client.get_entity(f"t.me/joinchat/{slug}")
                except Exception:
                    try:
                        entity = await self.ub.client.get_entity(int(slug))
                    except Exception:
                        return False
            else:
                # username أو chat_id
                t = str(target).strip().lstrip("@")
                try:
                    entity = await self.ub.client.get_entity(int(t))
                except (ValueError, TypeError):
                    entity = await self.ub.client.get_entity(t)
            await self.ub.client(LeaveChannelRequest(entity))
            logger.info(f"[{self.ub.phone}] ✅ غادر: {target}")
            return True
        except Exception as e:
            logger.error(f"[{self.ub.phone}] فشل leave {target}: {e}")
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
    """تحميل حساب من Session String في متغيرات البيئة وتسجيله في DB إن لم يكن موجوداً"""
    phone_label = f"ENV_SESSION_{session_num}"
    ub = UserBotClient(0, phone_label, DEFAULT_API_ID, DEFAULT_API_HASH, f"env_session_{session_num}")
    try:
        if not await ub.connect_with_string(session_string):
            logger.warning(f"⚠️ SESSION_{session_num} فشل الاتصال")
            return False

        me = ub.me
        username  = me.username if me else None
        full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() if me else phone_label

        # تحقق إذا كان الحساب موجوداً في DB مسبقاً
        cur = await _db.execute("SELECT id FROM accounts WHERE session_name=?", (f"env_session_{session_num}",))
        row = await cur.fetchone()
        if row:
            real_id = row[0]
            await db_update_account(real_id, username=username, full_name=full_name, is_active=1)
        else:
            real_id = await db_add_account(
                phone_label, str(DEFAULT_API_ID), DEFAULT_API_HASH,
                f"env_session_{session_num}",
                username=username, full_name=full_name,
                notes=f"Session String #{session_num}"
            )

        ub.account_id = real_id
        async with _mgr_lock:
            _clients[real_id] = ub
            _actions[real_id] = UserBotActions(ub)
        logger.info(f"✅ SESSION_{session_num} ({full_name}) متصل — DB ID: {real_id}")
        return True
    except Exception as e:
        logger.error(f"❌ SESSION_{session_num} خطأ: {e}")
        return False


async def load_session_from_bot(session_string: str, added_by: int = 0) -> dict:
    """
    إضافة حساب عبر Session String مباشرة من داخل البوت.
    يتحقق من الـ string، يتصل، ويخزن في DB.
    """
    session_string = session_string.strip()
    if len(session_string) < 50:
        return {"status": "error", "message": "Session String قصيرة جداً أو غير صحيحة"}

    # اشتق اسم جلسة فريد من أول 12 حرف
    sname = f"bot_ss_{session_string[:12].replace('/', '_').replace('+', '_')}"

    # تحقق أن الـ session مش مضافة قبل كده
    cur = await _db.execute("SELECT id, full_name FROM accounts WHERE session_name=?", (sname,))
    existing = await cur.fetchone()
    if existing:
        return {
            "status": "duplicate",
            "message": f"هذه الجلسة مضافة مسبقاً",
            "account_id": existing[0],
            "name": existing[1],
        }

    ub = UserBotClient(0, f"BOT_SS_{sname}", DEFAULT_API_ID, DEFAULT_API_HASH, sname)
    try:
        connected = await ub.connect_with_string(session_string)
        if not connected:
            return {"status": "error", "message": "فشل الاتصال — تأكد أن الـ Session String صحيحة وغير منتهية"}

        me = ub.me
        username  = me.username if me else None
        full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() if me else sname
        phone     = str(me.phone) if me and me.phone else sname

        real_id = await db_add_account(
            phone, str(DEFAULT_API_ID), DEFAULT_API_HASH, sname,
            username=username, full_name=full_name,
            notes=f"أُضيف عبر البوت بواسطة {added_by}"
        )
        ub.account_id = real_id
        async with _mgr_lock:
            _clients[real_id] = ub
            _actions[real_id] = UserBotActions(ub)

        await db_add_notification("new_account", "حساب جديد عبر Session String", full_name)
        logger.info(f"✅ Session String جديد ({full_name}) أُضيف عبر البوت — DB ID: {real_id}")
        return {
            "status": "success",
            "account_id": real_id,
            "name": full_name,
            "username": username,
            "phone": phone,
        }
    except Exception as e:
        logger.error(f"❌ load_session_from_bot خطأ: {e}")
        try:
            await ub.disconnect()
        except Exception:
            pass
        return {"status": "error", "message": str(e)}

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
        try: await ub.disconnect()
        except Exception: pass
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
    _buckets.pop(aid, None)
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
            "is_env":    ub.phone.startswith("ENV_SESSION_"),
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
            ok = False
            if task_type == "send_message":
                ok = await actions.send_message(target, content or "", parse_mode)
            elif task_type == "join_group":
                ok = await actions.join_group(target)
            elif task_type == "leave_group":
                ok = await actions.leave_group(target)
            elif task_type == "forward":
                parts = (content or "").split(":")
                ok = await actions.forward_message(parts[0], int(parts[1]), target) if len(parts)==2 else False
            status = "success" if ok else "failed"
            msg    = "تم التنفيذ" if ok else "فشل التنفيذ"
            await _db.execute("UPDATE tasks SET run_count=run_count+1 WHERE id=?", (tid,))
            await _db.commit()
        except Exception as e:
            status = "failed"
            msg    = str(e)
            logger.error(f"❌ استثناء في مهمة {tid}: {e}")
    ms = int((time.monotonic()-t0)*1000)
    await db_log(tid, sid, aid, status, msg, ms)
    await db_update_schedule_run(sid)
    icon = "✅" if status=="success" else "❌"
    logger.info(f"{icon} مهمة {tid}: {status} ({ms}ms)")

async def sched_add(tid, ttype, tdata, max_runs=-1) -> int:
    task = await db_get_task(tid)
    if not task: raise ValueError("مهمة غير موجودة")
    trigger = _build_trigger(ttype, tdata)
    if not trigger:
        raise ValueError("بيانات الجدول غير صالحة")
    sid = await db_add_schedule(tid, ttype, tdata, max_runs)
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
    return [
        {"id": j.id, "name": j.name, "next": str(j.next_run_time)[:16] if j.next_run_time else "—"}
        for j in _scheduler.get_jobs()
    ]

# ══════════════════════════════════════════════
#  🎛 Keyboards
# ══════════════════════════════════════════════

def kb_main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👤 الحسابات",      callback_data="menu_accounts"),
        InlineKeyboardButton("⚙️ المهام",         callback_data="menu_tasks"),
        InlineKeyboardButton("📅 الجداول",        callback_data="menu_schedules"),
        InlineKeyboardButton("📊 الإحصائيات",    callback_data="menu_stats"),
        InlineKeyboardButton("📋 السجلات",       callback_data="menu_logs"),
        InlineKeyboardButton("🛡️ الحماية",        callback_data="menu_protection"),
    )
    kb.row(
        InlineKeyboardButton("🔔 الإشعارات",     callback_data="menu_notifications"),
        InlineKeyboardButton("🤝 كلّم صاحب",     callback_data="menu_dm"),
    )
    kb.row(
        InlineKeyboardButton("👥 أصدقائي وجروباتي", callback_data="menu_social"),
    )
    kb.row(
        InlineKeyboardButton("🔄 تحديث القائمة", callback_data="back_main"),
        InlineKeyboardButton("🔁 إعادة تشغيل البوت", callback_data="bot_restart"),
    )
    return kb

def kb_accounts_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📋 عرض الكل",         callback_data="acc_list"),
        InlineKeyboardButton("➕ إضافة بهاتف",       callback_data="acc_add"),
        InlineKeyboardButton("🔑 إضافة بـ Session",  callback_data="acc_add_ss"),
        InlineKeyboardButton("📊 إحصائيات",          callback_data="acc_stats"),
        InlineKeyboardButton("◀️ رجوع",              callback_data="back_main"),
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
        notif_badge = f" 🔔`{len(notifications)}`" if notifications else ""
        connected   = mgr_count()
        jobs        = len(sched_jobs())
        now_str     = datetime.now(TZ).strftime("%H:%M  %d/%m/%Y")
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚡  TG MANAGER PRO v3   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🟢 حسابات متصلة: *{connected}*\n"
            f"📅 جداول نشطة:  *{jobs}*{notif_badge}\n"
            f"🕐 `{now_str}`\n\n"
            f"_اختر من القائمة 👇_"
        )
        bot.send_message(msg.chat.id, text, parse_mode="Markdown", reply_markup=kb_main_menu())

    # ── Callback: القوائم الرئيسية ──

    @bot.callback_query_handler(func=lambda c: c.data == "back_main")
    def cb_main(call: CallbackQuery):
        if not is_admin(call): return
        notifications = arun(db_get_unread_notifications())
        notif_badge = f" 🔔`{len(notifications)}`" if notifications else ""
        now_str     = datetime.now(TZ).strftime("%H:%M  %d/%m/%Y")
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚡  TG MANAGER PRO v3   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🟢 حسابات متصلة: *{mgr_count()}*\n"
            f"📅 جداول نشطة:  *{len(sched_jobs())}*{notif_badge}\n"
            f"🕐 `{now_str}`\n\n"
            f"_اختر من القائمة 👇_"
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

    @bot.callback_query_handler(func=lambda c: c.data == "bot_restart")
    def cb_bot_restart(call: CallbackQuery):
        if not is_admin(call): return
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔁  إعادة تشغيل البوت   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⚠️ هل تريد إعادة تشغيل البوت الآن؟\n\n"
            f"_سيتم قطع الاتصال مؤقتاً ثم العودة_",
            kb_confirm("bot_restart_confirm", "back_main")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "bot_restart_confirm")
    def cb_bot_restart_confirm(call: CallbackQuery):
        if not is_admin(call): return
        bot.answer_callback_query(call.id, "🔁 جاري إعادة التشغيل...")
        try:
            bot.edit_message_text(
                f"`╔══════════════════════════╗`\n"
                f"`║  🔁  جاري إعادة التشغيل  ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"⏳ _البوت سيعود خلال ثوانٍ..._",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown"
            )
        except Exception: pass
        logger.info(f"🔁 إعادة تشغيل بطلب من {call.from_user.id}")
        def _restart():
            import time as _t
            _t.sleep(1.5)
            try: _scheduler.shutdown(wait=False)
            except Exception: pass
            os.execv(sys.executable, [sys.executable] + sys.argv)
        threading.Thread(target=_restart, daemon=True).start()

    # ══════════════════════════════════════════
    #  🤝 إرسال رسالة لصاحب (DM Friends)
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_dm")
    def cb_menu_dm(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  🤝  إرسال لصاحب         ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"❌ _لا توجد حسابات متصلة_\n\nأضف حساباً أو تحقق من SESSION_",
                kb_back("back_main")
            )
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step": "dm_pick_account"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"dm_acc_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🤝  إرسال لصاحب - 1/3   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📱 اختر الحساب اللي هيبعت منه 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("dm_acc_"))
    def cb_dm_acc(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        aid = int(call.data.split("_", 2)[2])
        _states[uid]["account_id"] = aid
        _states[uid]["step"] = "dm_target"
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🤝  إرسال لصاحب - 2/3   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"👤 ابعت يوزر أو ID الشخص اللي تبي تكلمه:\n\n"
            f"• `@username`\n"
            f"• `123456789` (User ID)\n\n"
            f"_ينفع تكلم أي شخص حتى لو مش في قائمتك_ ✅"
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "dm_target"))
    def dm_step_target(msg: Message):
        uid = msg.from_user.id
        target = msg.text.strip()
        _states[uid]["target"] = target
        _states[uid]["step"]   = "dm_text"
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  🤝  إرسال لصاحب - 3/3   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"✏️ اكتب الرسالة اللي تبي تبعتها لـ `{target}` 👇",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "dm_text"))
    def dm_step_text(msg: Message):
        uid   = msg.from_user.id
        state = _states.get(uid, {})
        aid    = state.get("account_id")
        target = state.get("target")
        text   = msg.text
        clear_state(uid)
        actions = mgr_actions(aid)
        if not actions:
            return bot.send_message(msg.chat.id, "❌ *الحساب غير متصل*", parse_mode="Markdown")
        m = bot.send_message(msg.chat.id, S.loading(f"جاري الإرسال إلى {target}"), parse_mode="Markdown")
        ok = arun(actions.send_message(target, text, parse_mode="markdown"))
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🤝 إرسال آخر", callback_data="menu_dm"),
            InlineKeyboardButton("🏠 الرئيسية",  callback_data="back_main"),
        )
        result = (
            f"✅ *تم الإرسال بنجاح!*\n\n"
            f"👤 إلى: `{target}`\n"
            f"📱 من الحساب: `{aid}`\n\n"
            f"💬 _{text[:80]}{'...' if len(text)>80 else ''}_"
        ) if ok else (
            f"❌ *فشل الإرسال!*\n\n"
            f"👤 الهدف: `{target}`\n"
            f"_تأكد من صحة اليوزر/ID_"
        )
        try:
            bot.edit_message_text(result, m.chat.id, m.message_id, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            bot.send_message(msg.chat.id, result, parse_mode="Markdown", reply_markup=kb)

    # ══════════════════════════════════════════
    #  👤 الحسابات
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_accounts")
    def cb_menu_accounts(call: CallbackQuery):
        if not is_admin(call): return
        connected = mgr_count()
        accounts = arun(db_get_all_accounts(active_only=False))
        total = len(accounts)
        banned = sum(1 for a in accounts if a.get("is_banned"))
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  👤  إدارة الحسابات      ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📊 الإجمالي:  `{total}`\n"
            f"🟢 متصل:     `{connected}`\n"
            f"🔴 منفصل:    `{total - connected - banned}`\n"
            f"🚫 محظور:    `{banned}`\n\n"
            f"_اختر ما تريد 👇_"
        )
        edit_or_send(call, text, kb_accounts_menu())
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "acc_list")
    def cb_acc_list(call: CallbackQuery):
        if not is_admin(call): return
        bot.answer_callback_query(call.id, "⏳ جاري التحميل...")
        try:
            db_accounts = arun(db_get_all_accounts(active_only=False))
        except Exception as e:
            edit_or_send(call, f"❌ *خطأ في تحميل الحسابات:* `{e}`", kb_back("menu_accounts"))
            return
        db_ids = {a["id"] for a in db_accounts}
        # أضف أي حساب في _clients غير موجود في DB (Session String ENV)
        all_accounts = list(db_accounts)
        for aid, ub in _clients.items():
            if aid not in db_ids:
                all_accounts.append({
                    "id": aid,
                    "phone": ub.phone,
                    "full_name": ub.display_name(),
                    "username": ub._me.username if ub._me else None,
                    "is_banned": 0,
                    "msg_count": 0,
                    "join_count": 0,
                })

        if not all_accounts:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  👤  الحسابات            ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد حسابات مضافة بعد_\n\n"
                f"اضغط ➕ لإضافة حساب جديد",
                kb_accounts_menu()
            )
            return

        connected_count = sum(1 for a in all_accounts if mgr_is_connected(a["id"]))
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  👤  قائمة الحسابات      ║`",
            f"`╚══════════════════════════╝`\n",
            f"📊 الإجمالي: `{len(all_accounts)}` | 🟢 متصل: `{connected_count}`\n",
        ]
        for a in all_accounts:
            is_conn = mgr_is_connected(a["id"])
            badge = "🟢" if is_conn else ("🚫" if a.get("is_banned") else "🔴")
            raw_name = a.get("full_name") or a.get("phone", "—")
            name = raw_name.replace("_", r"\_").replace("*", r"\*")
            uname = f"  @{a['username']}" if a.get("username") else ""
            env_tag = " 🔑" if str(a.get("phone","")).startswith(("ENV_SESSION_","BOT_SS_")) else ""
            lines.append(
                f"{badge} *{name}*{uname}{env_tag}\n"
                f"  🆔 `{a['id']}` | 📱 `{str(a.get('phone','—'))[:20]}`\n"
                f"  💬 `{a.get('msg_count',0)}` رسالة | 🔗 `{a.get('join_count',0)}` انضمام"
            )

        kb = InlineKeyboardMarkup(row_width=2)
        for a in all_accounts[:10]:
            icon = "🟢" if mgr_is_connected(a["id"]) else ("🚫" if a.get("is_banned") else "🔴")
            nm = (a.get("full_name") or a.get("phone","—"))[:18]
            kb.add(InlineKeyboardButton(f"{icon} {nm}", callback_data=f"acc_detail_{a['id']}"))
        kb.row(
            InlineKeyboardButton("➕ إضافة", callback_data="acc_add"),
            InlineKeyboardButton("🔄 تحديث", callback_data="acc_list"),
            InlineKeyboardButton("◀️ رجوع",  callback_data="menu_accounts"),
        )
        edit_or_send(call, "\n".join(lines), kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_detail_"))
    def cb_acc_detail(call: CallbackQuery):
        if not is_admin(call): return
        try:
            aid = int(call.data.split("_", 2)[2])
        except (IndexError, ValueError):
            bot.answer_callback_query(call.id, "❌ بيانات خاطئة")
            return
        try:
            acc = arun(db_get_account(aid))
            ub  = mgr_client(aid)
            # لو مش في DB ابحث في الذاكرة (Session String ENV)
            if not acc:
                if ub:
                    acc = {
                        "id": aid, "phone": ub.phone,
                        "full_name": ub.display_name(),
                        "username": ub._me.username if ub._me else None,
                        "is_banned": 0, "msg_count": 0, "join_count": 0,
                        "created_at": "", "last_used": "",
                    }
                else:
                    bot.answer_callback_query(call.id, "❌ الحساب غير موجود")
                    edit_or_send(call, "❌ *الحساب غير موجود أو تم حذفه*", kb_back("acc_list"))
                    return
            connected = mgr_is_connected(aid)
            badge     = "🟢 متصل" if connected else ("🚫 محظور" if acc.get("is_banned") else "🔴 منفصل")
            text = (
                f"`╔══════════════════════════╗`\n"
                f"`║  👤  تفاصيل الحساب       ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"*الاسم:*   {acc.get('full_name','—')}\n"
                f"*يوزر:*   {'@'+acc['username'] if acc.get('username') else '—'}\n"
                f"*هاتف:*   `{acc['phone']}`\n"
                f"*ID:*       `{acc['id']}`\n\n"
                f"┌─ *الحالة* ───────────────\n"
                f"│  {badge}\n"
                f"│  ⏱️ Uptime: `{ub.uptime() if ub else '—'}`\n"
                f"│  🔄 إعادة اتصال: `{ub.reconnect_count if ub else 0}` مرة\n"
                f"└──────────────────────────\n\n"
                f"📈 *الإحصائيات:*\n"
                f"  💬 رسائل مُرسلة: `{acc.get('msg_count',0)}`\n"
                f"  🔗 انضمامات:     `{acc.get('join_count',0)}`\n"
                f"  📅 أُضيف: {S.format_time(acc.get('created_at',''))}\n"
                f"  🕐 آخر نشاط: {S.format_time(acc.get('last_used','')) if acc.get('last_used') else '—'}"
            )
            edit_or_send(call, text, kb_account_actions(aid))
            bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"cb_acc_detail error: {e}")
            try:
                bot.answer_callback_query(call.id, "❌ خطأ في تحميل بيانات الحساب")
                edit_or_send(call,
                    f"❌ *خطأ في تحميل الحساب* `#{aid}`\n\n`{e}`",
                    kb_back("acc_list")
                )
            except Exception:
                pass

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_reconnect_"))
    def cb_acc_reconnect(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_", 2)[2])
        bot.answer_callback_query(call.id, "🔄 جاري إعادة الاتصال...")
        acc = arun(db_get_account(aid))
        if not acc:
            bot.send_message(call.message.chat.id, "❌ *الحساب غير موجود*", parse_mode="Markdown")
            return
        async def _reconnect():
            if aid in _clients:
                await _clients[aid].disconnect()
                _clients.pop(aid, None)
                _actions.pop(aid, None)
            return await _start_acc(acc)
        ok = arun(_reconnect())
        bot.send_message(
            call.message.chat.id,
            f"✅ *الحساب `#{aid}` أُعيد اتصاله بنجاح!*" if ok
            else f"❌ *فشل إعادة الاتصال للحساب `#{aid}`*",
            parse_mode="Markdown"
        )

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_dialogs_"))
    def cb_acc_dialogs(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_", 2)[2])
        actions = mgr_actions(aid)
        if not actions:
            bot.answer_callback_query(call.id, "❌ الحساب غير متصل")
            return
        bot.answer_callback_query(call.id, "⏳ جاري التحميل...")
        dialogs = arun(actions.get_dialogs(20))
        if not dialogs:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  💬  المحادثات            ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد محادثات_",
                kb_back(f"acc_detail_{aid}")
            )
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  💬  محادثات #{aid}         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for d in dialogs[:20]:
            name  = getattr(d.entity,"title",None) or getattr(d.entity,"first_name","؟")
            eid   = d.entity.id
            uname = f"  @{d.entity.username}" if getattr(d.entity,"username",None) else ""
            lines.append(f"• `{eid}` *{name}*{uname}")
        edit_or_send(call, "\n".join(lines), kb_back(f"acc_detail_{aid}"))

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_delete_"))
    def cb_acc_delete(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_", 2)[2])
        edit_or_send(
            call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚠️  تأكيد الحذف          ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"هل تريد حذف الحساب `#{aid}` نهائيًا؟\n\n"
            f"_سيتم حذف جميع مهامه وجداوله أيضًا!_",
            kb_confirm(f"acc_confirm_delete_{aid}", "acc_list")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_confirm_delete_"))
    def cb_acc_confirm_delete(call: CallbackQuery):
        if not is_admin(call): return
        try:
            aid = int(call.data.split("_", 3)[3])
        except (ValueError, IndexError):
            bot.answer_callback_query(call.id, "❌ بيانات خاطئة")
            return
        arun(mgr_remove(aid))
        edit_or_send(call,
            f"✅ *تم حذف الحساب `#{aid}` بنجاح*",
            kb_back("acc_list")
        )
        bot.answer_callback_query(call.id, "✅ تم الحذف")

    @bot.callback_query_handler(func=lambda c: c.data == "acc_stats")
    def cb_acc_stats(call: CallbackQuery):
        if not is_admin(call): return
        statuses = mgr_all_status()
        if not statuses:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  📊  حالة الحسابات        ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد حسابات_",
                kb_back("menu_accounts")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  📊  حالة الحسابات        ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for s in statuses:
            icon = "🟢" if s["connected"] else "🔴"
            lines.append(
                f"{icon} *{s['name']}*\n"
                f"  ⏱️ Uptime: `{s['uptime']}` | 🔄 Reconnects: `{s['reconnects']}`"
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
            f"`╔══════════════════════════╗`\n"
            f"`║  👤  إضافة حساب - 1/3    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📱 أرسل رقم الهاتف بالصيغة الدولية:\n\n"
            f"مثال: `+201012345678`\n\n"
            f"_أو اضغط إلغاء_",
            kb_back("menu_accounts")
        )
        bot.answer_callback_query(call.id)

    # ══════════════════════════════════════════
    #  🔑 إضافة حساب بـ Session String مباشرة
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "acc_add_ss")
    def cb_acc_add_ss(call: CallbackQuery):
        if not is_admin(call): return
        uid = call.from_user.id
        set_state(uid, {"step": "acc_ss_input"})
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔑  إضافة بـ Session     ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📋 أرسل الـ *Session String* مباشرة\n\n"
            f"_هو نص طويل تحصل عليه من `Pyrogram`، `Telethon`، أو أدوات توليد الجلسات_\n\n"
            f"⚠️ لا تشارك هذا المفتاح مع أي أحد غيرك\n\n"
            f"_أو اضغط إلغاء_",
            kb_back("menu_accounts")
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "acc_ss_input"))
    def acc_step_ss_input(msg: Message):
        if not is_admin(msg): return
        uid = msg.from_user.id
        ss  = msg.text.strip()
        clear_state(uid)

        # احذف رسالة الـ session string فوراً للأمان
        try:
            bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass

        m = bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔑  جاري الاتصال...      ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⏳ _جاري التحقق من الـ Session String..._",
            parse_mode="Markdown"
        )

        result = arun(load_session_from_bot(ss, added_by=uid))

        if result["status"] == "success":
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("👤 تفاصيل الحساب", callback_data=f"acc_detail_{result['account_id']}"),
                InlineKeyboardButton("◀️ الحسابات",       callback_data="acc_list"),
            )
            bot.edit_message_text(
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  تم إضافة الحساب!    ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"👤 *الاسم:*  {result['name']}\n"
                f"🔗 *يوزر:* {'@'+result['username'] if result.get('username') else '—'}\n"
                f"📱 *هاتف:*  `{result.get('phone','—')}`\n"
                f"🆔 *ID:*     `{result['account_id']}`\n\n"
                f"🚀 _الحساب متصل وجاهز للاستخدام!_",
                m.chat.id, m.message_id,
                parse_mode="Markdown", reply_markup=kb
            )
        elif result["status"] == "duplicate":
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("👤 عرض الحساب", callback_data=f"acc_detail_{result['account_id']}"))
            bot.edit_message_text(
                f"`╔══════════════════════════╗`\n"
                f"`║  ⚠️  جلسة مكررة           ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"هذه الجلسة مضافة مسبقاً:\n"
                f"👤 *{result['name']}* `#{result['account_id']}`",
                m.chat.id, m.message_id,
                parse_mode="Markdown", reply_markup=kb
            )
        else:
            bot.edit_message_text(
                f"`╔══════════════════════════╗`\n"
                f"`║  ❌  فشل الإضافة          ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"*السبب:*\n_{result.get('message','خطأ غير معروف')}_\n\n"
                f"تأكد أن الـ Session String:\n"
                f"• صحيحة وغير منتهية\n"
                f"• من حساب Telegram فعّال\n"
                f"• ليست محظورة",
                m.chat.id, m.message_id,
                parse_mode="Markdown",
                reply_markup=kb_back("menu_accounts")
            )

    @bot.message_handler(commands=["addsession"])
    def cmd_addsession(msg: Message):
        """إضافة حساب بـ Session String عبر أمر نصي مباشر"""
        if not is_admin(msg): return
        parts = msg.text.split(maxsplit=1)

        # احذف الرسالة فوراً للأمان
        try:
            bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass

        if len(parts) < 2 or len(parts[1].strip()) < 50:
            return bot.send_message(msg.chat.id,
                f"`╔══════════════════════════╗`\n"
                f"`║  🔑  إضافة بـ Session     ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"*الاستخدام:*\n`/addsession <session_string>`\n\n"
                f"أو استخدم زر *🔑 إضافة بـ Session* من قائمة الحسابات",
                parse_mode="Markdown",
                reply_markup=kb_back("menu_accounts")
            )

        ss = parts[1].strip()
        m  = bot.send_message(msg.chat.id,
            f"⏳ _جاري التحقق من الـ Session String..._",
            parse_mode="Markdown"
        )
        result = arun(load_session_from_bot(ss, added_by=msg.from_user.id))

        if result["status"] == "success":
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("👤 تفاصيل الحساب", callback_data=f"acc_detail_{result['account_id']}"),
                InlineKeyboardButton("◀️ الحسابات",       callback_data="acc_list"),
            )
            bot.edit_message_text(
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  تم إضافة الحساب!    ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"👤 *{result['name']}*\n"
                f"🔗 {'@'+result['username'] if result.get('username') else '—'}\n"
                f"📱 `{result.get('phone','—')}`\n"
                f"🆔 `{result['account_id']}`\n\n"
                f"🚀 _متصل وجاهز!_",
                m.chat.id, m.message_id,
                parse_mode="Markdown", reply_markup=kb
            )
        elif result["status"] == "duplicate":
            bot.edit_message_text(
                f"⚠️ *جلسة مكررة* — مضافة مسبقاً\n"
                f"👤 *{result['name']}* `#{result['account_id']}`",
                m.chat.id, m.message_id, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("👤 عرض", callback_data=f"acc_detail_{result['account_id']}")
                )
            )
        else:
            bot.edit_message_text(
                f"❌ *فشل:* _{result.get('message','خطأ')}_",
                m.chat.id, m.message_id, parse_mode="Markdown",
                reply_markup=kb_back("menu_accounts")
            )
    @bot.message_handler(commands=["addaccount"])
    def cmd_addacc(msg: Message):
        if not is_admin(msg): return
        set_state(msg.from_user.id, {"step":"acc_phone"})
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  👤  إضافة حساب - 1/3    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📱 أرسل رقم الهاتف:\n`+201012345678`",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_phone"))
    def acc_step_phone(msg: Message):
        p = msg.text.strip()
        if not p.startswith("+") or not p[1:].replace(" ","").isdigit():
            return bot.reply_to(msg, "❌ صيغة خاطئة.\nمثال: `+201012345678`", parse_mode="Markdown")
        _states[msg.from_user.id]["phone"] = p
        _states[msg.from_user.id]["step"]  = "acc_api_id"
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  👤  إضافة حساب - 2/3    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🔑 روح *my.telegram.org* وأرسل الـ *API ID*\n\n"
            f"_رقم مكون من أرقام فقط_",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id,"acc_api_id"))
    def acc_step_api_id(msg: Message):
        try:
            api_id = int(msg.text.strip())
        except ValueError:
            return bot.reply_to(msg, "❌ API ID يجب أن يكون أرقاماً فقط")
        _states[msg.from_user.id]["api_id"] = api_id
        _states[msg.from_user.id]["step"]   = "acc_api_hash"
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  👤  إضافة حساب - 3/3    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🔐 أرسل الـ *API Hash*\n\n"
            f"_سلسلة حروف وأرقام من my.telegram.org_",
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
                f"`╔══════════════════════════╗`\n"
                f"`║  📨  كود التحقق           ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"✅ تم إرسال الكود إلى:\n`{state['phone']}`\n\n"
                f"📲 أرسل الكود هنا 👇\n"
                f"_شكله: `12345` أو `1 2 3 4 5`_",
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
        state["code"] = code  # حفظ الكود للـ 2FA
        result = arun(mgr_complete_login(state["phone"], code))
        if result["status"] == "success":
            clear_state(uid)
            aid = result["account_id"]
            bot.send_message(msg.chat.id,
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  تم تسجيل الدخول!    ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"👤 *الاسم:*  {result['name']}\n"
                f"🆔 *ID:*     `{aid}`\n"
                f"🔗 *يوزر:*  {'@'+result['username'] if result.get('username') else '—'}\n\n"
                f"🎉 _الحساب جاهز للاستخدام!_",
                parse_mode="Markdown",
                reply_markup=kb_back("menu_accounts")
            )
        elif result["status"] == "need_2fa":
            _states[uid]["step"] = "acc_2fa"
            bot.send_message(msg.chat.id,
                f"`╔══════════════════════════╗`\n"
                f"`║  🔐  التحقق الثنائي 2FA  ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"أرسل كلمة مرور الـ 2FA:",
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
                f"✅ *مرحبًا {result['name']}!*\n🆔 `{result['account_id']}`",
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
        try:
            tasks = arun(db_get_all_tasks())
        except Exception as e:
            edit_or_send(call, f"❌ *خطأ في تحميل المهام:* `{e}`", kb_back("back_main"))
            bot.answer_callback_query(call.id)
            return
        type_counts = {}
        for t in tasks:
            type_counts[t["task_type"]] = type_counts.get(t["task_type"], 0) + 1
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚙️  إدارة المهام         ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📊 إجمالي المهام: `{len(tasks)}`\n"
        )
        if type_counts:
            text += "\n*التوزيع:*\n"
            for tt, cnt in type_counts.items():
                text += f"  {S.task_type_icon(tt)} {S.task_type_ar(tt)}: `{cnt}`\n"
        text += "\n_اختر ما تريد 👇_"
        edit_or_send(call, text, kb_tasks_menu())
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "task_list")
    def cb_task_list(call: CallbackQuery):
        if not is_admin(call): return
        bot.answer_callback_query(call.id, "⏳ جاري التحميل...")
        try:
            tasks = arun(db_get_all_tasks())
        except Exception as e:
            edit_or_send(call, f"❌ *خطأ:* `{e}`", kb_back("menu_tasks"))
            return
        if not tasks:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  ⚙️  المهام               ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد مهام بعد_\n\nاضغط ➕ لإنشاء مهمة",
                kb_tasks_menu()
            )
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  ⚙️  قائمة المهام         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for t in tasks:
            icon  = S.task_type_icon(t["task_type"])
            tname = S.task_type_ar(t["task_type"])
            acc   = t.get("full_name") or t.get("phone", "?")
            lines.append(
                f"{icon} *{t['name']}* `#{t['id']}`\n"
                f"  👤 {acc} → `{t['target'][:25]}`\n"
                f"  🏷️ {tname} | ▶️ `{t.get('run_count',0)}` مرة"
            )
        kb = InlineKeyboardMarkup(row_width=2)
        for t in tasks[:8]:
            icon = S.task_type_icon(t["task_type"])
            kb.add(InlineKeyboardButton(
                f"{icon} {t['name'][:20]} #{t['id']}",
                callback_data=f"task_detail_{t['id']}"
            ))
        kb.row(
            InlineKeyboardButton("➕ جديد",  callback_data="task_add"),
            InlineKeyboardButton("🔄 تحديث", callback_data="task_list"),
            InlineKeyboardButton("◀️ رجوع",  callback_data="menu_tasks"),
        )
        edit_or_send(call, "\n".join(lines), kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_detail_"))
    def cb_task_detail(call: CallbackQuery):
        if not is_admin(call): return
        tid  = int(call.data.split("_", 2)[2])
        task = arun(db_get_task(tid))
        if not task:
            bot.answer_callback_query(call.id, "❌ المهمة غير موجودة")
            return
        acc  = arun(db_get_account(task["account_id"]))
        icon = S.task_type_icon(task["task_type"])
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  {icon}  تفاصيل المهمة       ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"*الاسم:*    {task['name']}\n"
            f"*النوع:*    {S.task_type_ar(task['task_type'])}\n"
            f"*الهدف:*    `{task['target']}`\n"
            f"*الحساب:*  {acc.get('full_name','?') if acc else '?'} `#{task['account_id']}`\n"
            f"*ID:*        `{task['id']}`\n"
        )
        if task.get("content"):
            preview = task["content"][:100] + ("..." if len(task["content"]) > 100 else "")
            text += f"\n📝 *المحتوى:*\n`{preview}`\n"
        text += (
            f"\n┌─ *إحصائيات* ─────────────\n"
            f"│  ▶️ تنفيذات: `{task.get('run_count',0)}`\n"
            f"│  📅 أُنشئت: {S.format_time(task.get('created_at',''))}\n"
            f"└──────────────────────────"
        )
        edit_or_send(call, text, kb_task_actions(tid))
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_run_"))
    def cb_task_run(call: CallbackQuery):
        if not is_admin(call): return
        tid  = int(call.data.split("_", 2)[2])
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
        icon = S.task_type_icon(task["task_type"])
        result_text = (
            f"✅ *تم التنفيذ بنجاح!*\n\n"
            f"{icon} *{task['name']}* `#{tid}`\n"
            f"🎯 `{task['target']}`"
        ) if ok else (
            f"❌ *فشل التنفيذ!*\n\n"
            f"{icon} *{task['name']}* `#{tid}`\n"
            f"🎯 `{task['target']}`\n"
            f"_تحقق من الحساب والهدف_"
        )
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("▶️ تشغيل مجدداً", callback_data=f"task_run_{tid}"),
            InlineKeyboardButton("◀️ المهام",        callback_data="task_list"),
        )
        try:
            bot.edit_message_text(result_text, m.chat.id, m.message_id, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            bot.send_message(call.message.chat.id, result_text, parse_mode="Markdown", reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_delete_"))
    def cb_task_delete(call: CallbackQuery):
        if not is_admin(call): return
        tid = int(call.data.split("_", 2)[2])
        edit_or_send(
            call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚠️  تأكيد الحذف          ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"هل تريد حذف المهمة `#{tid}` نهائيًا؟\n\n"
            f"_سيتم حذف جداولها أيضًا!_",
            kb_confirm(f"task_confirm_delete_{tid}", "task_list")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_confirm_delete_"))
    def cb_task_confirm_delete(call: CallbackQuery):
        if not is_admin(call): return
        try:
            tid = int(call.data.split("_", 3)[3])
        except (ValueError, IndexError):
            bot.answer_callback_query(call.id, "❌ بيانات خاطئة")
            return
        arun(db_delete_task(tid))
        edit_or_send(call, f"✅ *تم حذف المهمة `{tid}` بنجاح*", kb_back("task_list"))
        bot.answer_callback_query(call.id, "✅ تم الحذف")

    # ── إنشاء مهمة (wizard) ──

    @bot.callback_query_handler(func=lambda c: c.data == "task_add")
    def cb_task_add(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  ❌  لا توجد حسابات       ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"_أضف حسابًا أولاً أو تحقق من SESSION_ في متغيرات البيئة_",
                kb_back("menu_accounts")
            )
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step":"task_account"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"task_acc_pick_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚙️  مهمة جديدة - 1/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"👤 اختر الحساب 👇",
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
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"task_acc_pick_{a['id']}"
            ))
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚙️  مهمة جديدة - 1/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"👤 اختر الحساب:",
            parse_mode="Markdown", reply_markup=kb
        )

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_acc_pick_"))
    def cb_task_acc_pick(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        # "task_acc_pick_5" → split("_", 3) → ["task","acc","pick","5"] → [3]="5"
        aid = int(call.data.split("_", 3)[3])
        _states[uid]["account_id"] = aid
        _states[uid]["step"]       = "task_type"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("💬 إرسال رسالة",  callback_data="task_type_pick_send_message"),
            InlineKeyboardButton("🔗 انضمام لجروب", callback_data="task_type_pick_join_group"),
            InlineKeyboardButton("🚪 مغادرة جروب",  callback_data="task_type_pick_leave_group"),
            InlineKeyboardButton("↪️ إعادة توجيه",  callback_data="task_type_pick_forward"),
            InlineKeyboardButton("❌ إلغاء",         callback_data="cancel"),
        )
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚙️  مهمة جديدة - 2/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"اختر *نوع* المهمة 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_type_pick_"))
    def cb_task_type_pick(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        ttype = "_".join(call.data.split("_")[3:])
        _states[uid]["task_type"] = ttype
        _states[uid]["step"]      = "task_name"
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚙️  مهمة جديدة - 3/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
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
            f"`╔══════════════════════════╗`\n"
            f"`║  ⚙️  مهمة جديدة - 4/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🎯 أرسل *الهدف*\n\n"
            f"• يوزر: `@groupname`\n"
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
                InlineKeyboardButton("▶️ تشغيل الآن", callback_data=f"task_run_{tid}"),
                InlineKeyboardButton("📅 جدولة",       callback_data=f"task_sched_{tid}"),
                InlineKeyboardButton("📋 كل المهام",   callback_data="task_list"),
            )
            clear_state(uid)
            bot.send_message(msg.chat.id,
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  المهمة أُنشئت!       ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"{icon} *{state['name']}*\n"
                f"🆔 `{tid}` | 🎯 `{state['target']}`\n"
                f"🏷️ {S.task_type_ar(state['task_type'])}\n\n"
                f"_اختر الخطوة التالية 👇_",
                parse_mode="Markdown", reply_markup=kb
            )
        except Exception as e:
            clear_state(uid)
            bot.send_message(msg.chat.id, S.error(str(e)), parse_mode="Markdown")

    # ══════════════════════════════════════════
    #  📅 الجداول
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_schedules")
    def cb_menu_schedules(call: CallbackQuery):
        if not is_admin(call): return
        jobs = sched_jobs()
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  📅  إدارة الجداول       ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⏰ جداول نشطة: `{len(jobs)}`\n"
            f"🤖 المجدول: `{'🟢 يعمل' if _scheduler.running else '🔴 متوقف'}`\n"
        )
        if jobs:
            text += "\n*أقرب 3 مهام:*\n"
            for j in jobs[:3]:
                text += f"  ⏭️ {j['name'][:20]} → `{str(j['next'])[:16]}`\n"
        text += "\n_اختر ما تريد 👇_"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("📋 عرض الكل",     callback_data="sched_list"),
            InlineKeyboardButton("➕ جدول جديد",    callback_data="sched_add_pick"),
            InlineKeyboardButton("🔄 إعادة تحميل",  callback_data="sched_reload"),
            InlineKeyboardButton("◀️ رجوع",          callback_data="back_main"),
        )
        edit_or_send(call, text, kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "sched_reload")
    def cb_sched_reload(call: CallbackQuery):
        if not is_admin(call): return
        bot.answer_callback_query(call.id, "🔄 جاري إعادة تحميل الجداول...")
        try:
            for job in _scheduler.get_jobs():
                try: _scheduler.remove_job(job.id)
                except Exception: pass
            arun(sched_load_all())
            jobs = sched_jobs()
            edit_or_send(call,
                f"✅ *تم إعادة تحميل الجداول!*\n\n"
                f"⏰ جداول نشطة: `{len(jobs)}`",
                kb_back("menu_schedules")
            )
        except Exception as e:
            edit_or_send(call, S.error(str(e)), kb_back("menu_schedules"))

    @bot.callback_query_handler(func=lambda c: c.data == "sched_list")
    def cb_sched_list(call: CallbackQuery):
        if not is_admin(call): return
        jobs = sched_jobs()
        db_scheds = arun(db_get_active_schedules())
        if not jobs and not db_scheds:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  📅  الجداول              ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد جداول نشطة_\n\n"
                f"اضغط ➕ لجدولة مهمة",
                kb_back("menu_schedules")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  📅  الجداول النشطة       ║`",
            f"`╚══════════════════════════╝`\n",
            f"⏰ في المجدول: `{len(jobs)}` | 🗄️ في DB: `{len(db_scheds)}`\n",
        ]
        for j in jobs:
            name_safe = j['name'].replace("_", r"\_").replace("*", r"\*")
            lines.append(
                f"⏰ *{name_safe}*\n"
                f"  🆔 `{j['id']}` | ⏭️ التالي: `{str(j['next'])[:16]}`"
            )
        kb = InlineKeyboardMarkup(row_width=1)
        for j in jobs[:8]:
            raw_id = j['id']
            db_id  = raw_id[1:] if raw_id.startswith("s") else raw_id
            nm = j['name'][:22]
            kb.add(InlineKeyboardButton(
                f"🗑️ حذف: {nm}",
                callback_data=f"sched_delete_{db_id}"
            ))
        kb.row(
            InlineKeyboardButton("🔄 تحديث", callback_data="sched_list"),
            InlineKeyboardButton("◀️ رجوع",  callback_data="menu_schedules"),
        )
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
        tid = int(call.data.split("_", 2)[2])
        set_state(uid, {"step":"sched_type","task_id":tid})
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("⏰ مرة واحدة (Once)",       callback_data="sc_t_once"),
            InlineKeyboardButton("🔁 تكرار بفترة (Interval)", callback_data="sc_t_interval"),
            InlineKeyboardButton("📆 جدول Cron متقدم",        callback_data="sc_t_cron"),
            InlineKeyboardButton("❌ إلغاء",                  callback_data="cancel"),
        )
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  📅  جدولة المهمة {tid}     ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"اختر *نوع* الجدولة 👇\n\n"
            f"⏰ *Once* — تشغيل مرة واحدة في وقت محدد\n"
            f"🔁 *Interval* — تكرار كل فترة زمنية\n"
            f"📆 *Cron* — جدول متقدم (مثل Linux cron)",
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
                f"`╔══════════════════════════╗`\n"
                f"`║  ⏰  توقيت التشغيل        ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📅 أرسل التاريخ والوقت:\n\n"
                f"`YYYY-MM-DD HH:MM`\n\n"
                f"مثال: `2025-12-25 09:00`"
            ),
            "interval": (
                f"`╔══════════════════════════╗`\n"
                f"`║  🔁  فترة التكرار         ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"أرسل الفترة الزمنية:\n\n"
                f"• `hours=2` — كل ساعتين\n"
                f"• `minutes=30` — كل 30 دقيقة\n"
                f"• `days=1` — كل يوم\n"
                f"• `hours=1 minutes=30` — كل ساعة ونصف"
            ),
            "cron": (
                f"`╔══════════════════════════╗`\n"
                f"`║  📆  Cron Expression      ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"أرسل تعبير Cron (5 حقول):\n\n"
                f"`دقيقة ساعة يوم شهر يوم_أسبوع`\n\n"
                f"أمثلة:\n"
                f"• `0 9 * * *` — كل يوم 9 صباحًا\n"
                f"• `0 * * * *` — كل ساعة\n"
                f"• `0 8 * * 1` — كل اثنين 8ص\n"
                f"• `*/30 * * * *` — كل 30 دقيقة"
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
            f"`╔══════════════════════════╗`\n"
            f"`║  🔢  عدد مرات التشغيل    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"كم مرة تريد تشغيل هذه المهمة؟\n\n"
            f"• رقم محدد مثل `5`\n"
            f"• `0` للتشغيل *اللانهائي* ♾️",
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
            runs_txt = "♾️ لانهائي" if n == 0 else f"`{n}` مرة"
            bot.send_message(msg.chat.id,
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  تم إنشاء الجدول!     ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"🆔 `{sid}` | ⚙️ المهمة: `{tid}`\n"
                f"🏷️ النوع: {S.trigger_type_ar(stype)}\n"
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
        try: sid = int(call.data.split("_", 2)[2])
        except (ValueError, IndexError): sid = 0
        edit_or_send(call,
            f"⚠️ *حذف الجدول `{sid}`؟*",
            kb_confirm(f"sched_confirm_{sid}", "sched_list")
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("sched_confirm_"))
    def cb_sched_confirm(call: CallbackQuery):
        if not is_admin(call): return
        try:
            sid = int(call.data.split("_", 2)[2])
        except (ValueError, IndexError):
            bot.answer_callback_query(call.id, "❌ بيانات خاطئة")
            return
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
        stats    = arun(db_get_stats())
        connected = mgr_count()
        total_acc = stats["total_accounts"]
        total_logs = max(stats["total_logs"], 1)
        success_rate = round(stats["ok_total"] / total_logs * 100, 1)

        bar_today = S.progress_bar(stats["ok_today"], max(stats["ok_today"]+stats["fail_today"], 1))
        bar_total = S.progress_bar(stats["ok_total"], total_logs)

        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  📊  إحصائيات النظام     ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"┌─ *👤 الحسابات* ───────────\n"
            f"│  إجمالي:  `{total_acc}` | متصل: `{connected}` 🟢\n"
            f"│  محظور:   `{stats['banned_accounts']}` 🚫\n"
            f"└──────────────────────────\n\n"
            f"┌─ *⚙️ المهام والجداول* ─────\n"
            f"│  مهام:    `{stats['total_tasks']}`\n"
            f"│  جداول:   `{stats['total_schedules']}`\n"
            f"└──────────────────────────\n\n"
            f"┌─ *📈 أداء اليوم* ──────────\n"
            f"│  ✅ `{stats['ok_today']}` نجح  |  ❌ `{stats['fail_today']}` فشل\n"
            f"│  `{bar_today}`\n"
            f"└──────────────────────────\n\n"
            f"┌─ *📊 الأداء الكلي* ────────\n"
            f"│  ✅ `{stats['ok_total']}` نجح  |  ❌ `{stats['fail_total']}` فشل\n"
            f"│  `{bar_total}` `{success_rate}%`\n"
            f"└──────────────────────────\n\n"
            f"💬 رسائل كلية: `{S.format_number(stats['total_msgs'])}`"
        )
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🔄 تحديث", callback_data="menu_stats"),
            InlineKeyboardButton("◀️ رجوع",  callback_data="back_main"),
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
                f"`╔══════════════════════════╗`\n"
                f"`║  📋  السجلات              ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد سجلات بعد_",
                kb_back("back_main")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  📋  آخر العمليات         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for l in logs:
            icon  = "✅" if l["status"] == "success" else "❌"
            tname = l.get("task_name", "؟")
            dur   = f"`{l.get('duration_ms',0)}ms`" if l.get("duration_ms") else ""
            lines.append(
                f"{icon} *{tname}* {dur}\n"
                f"  🕐 `{str(l['executed_at'])[:16]}` | 📱 {l.get('phone','—')}"
            )
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ الناجحة", callback_data="logs_success"),
            InlineKeyboardButton("❌ الفاشلة", callback_data="logs_failed"),
            InlineKeyboardButton("🔄 تحديث",   callback_data="menu_logs"),
            InlineKeyboardButton("◀️ رجوع",    callback_data="back_main"),
        )
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data in ("logs_success","logs_failed"))
    def cb_logs_filter(call: CallbackQuery):
        status = "success" if call.data == "logs_success" else "failed"
        logs   = arun(db_get_logs(15, status=status))
        icon   = "✅" if status == "success" else "❌"
        label  = "الناجحة" if status == "success" else "الفاشلة"
        if not logs:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  📋  السجلات {label}      ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد عمليات {icon}_",
                kb_back("menu_logs")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  📋  السجلات {label}      ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for l in logs:
            lines.append(
                f"{icon} *{l.get('task_name','؟')}*\n"
                f"  🕐 `{str(l['executed_at'])[:16]}` — _{l.get('message','') or '—'}_"
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
            f"`╔══════════════════════════╗`\n"
            f"`║  🛡️  إعدادات الحماية     ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"┌─ *القائمة السوداء* ─────────\n"
            f"│  🚫 عدد الأهداف: `{len(bl)}`\n"
            f"└──────────────────────────\n\n"
            f"┌─ *Anti-Ban Settings* ───────\n"
            f"│  ⏱️ تأخير الإرسال: `{MIN_DELAY}-{MAX_DELAY}s`\n"
            f"│  ⏱️ تأخير الانضمام: `{JOIN_DELAY}s`\n"
            f"│  📊 حد الإرسال: `{RATE_MSGS}` رسالة/`{RATE_PERIOD}s`\n"
            f"└──────────────────────────"
        )
        if bl:
            text += "\n\n*آخر إضافات القائمة السوداء:*\n"
            for b in bl[:5]:
                text += f"  🚫 `{b['target']}` — _{b.get('reason','—')}_\n"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🚫 إضافة للقائمة",  callback_data="bl_add"),
            InlineKeyboardButton("✅ إزالة من القائمة", callback_data="bl_remove"),
            InlineKeyboardButton("📋 عرض القائمة",     callback_data="bl_list"),
            InlineKeyboardButton("◀️ رجوع",             callback_data="back_main"),
        )
        edit_or_send(call, text, kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "bl_add")
    def cb_bl_add(call: CallbackQuery):
        if not is_admin(call): return
        uid = call.from_user.id
        set_state(uid, {"step":"bl_add_target"})
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🚫  إضافة للقائمة السوداء║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"أرسل اليوزر أو الـ ID:\n`@username` أو `-1001234`"
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
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  🚫  القائمة السوداء      ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _القائمة السوداء فارغة_",
                kb_back("menu_protection")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  🚫  القائمة السوداء      ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for b in bl:
            lines.append(f"🚫 `{b['target']}` — _{b.get('reason','—')}_")
        edit_or_send(call, "\n".join(lines), kb_back("menu_protection"))
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data == "bl_remove")
    def cb_bl_remove(call: CallbackQuery):
        if not is_admin(call): return
        set_state(call.from_user.id, {"step": "bl_remove_target"})
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ✅  إزالة من القائمة     ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"أرسل الهدف المراد إزالته:\n`@username` أو `-1001234`"
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "bl_remove_target"))
    def bl_step_remove(msg: Message):
        uid    = msg.from_user.id
        target = msg.text.strip()
        clear_state(uid)
        arun(db_remove_blacklist(target))
        bot.send_message(msg.chat.id,
            f"✅ *تم إزالة* `{target}` *من القائمة السوداء*",
            parse_mode="Markdown", reply_markup=kb_back("menu_protection")
        )

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_tasks_"))
    def cb_acc_tasks(call: CallbackQuery):
        if not is_admin(call): return
        bot.answer_callback_query(call.id, "⏳ جاري التحميل...")
        aid   = int(call.data.split("_", 2)[2])
        tasks = arun(db_get_all_tasks(account_id=aid))
        if not tasks:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  ⚙️  مهام الحساب          ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد مهام للحساب `#{aid}`_",
                kb_back(f"acc_detail_{aid}")
            )
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  ⚙️  مهام الحساب #{aid}     ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        kb = InlineKeyboardMarkup(row_width=1)
        for t in tasks[:8]:
            icon = S.task_type_icon(t["task_type"])
            lines.append(f"{icon} `#{t['id']}` *{t['name']}* → `{t['target'][:20]}`")
            kb.add(InlineKeyboardButton(
                f"{icon} {t['name'][:25]} (#{t['id']})",
                callback_data=f"task_detail_{t['id']}"
            ))
        kb.add(InlineKeyboardButton("◀️ رجوع", callback_data=f"acc_detail_{aid}"))
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("task_edit_"))
    def cb_task_edit(call: CallbackQuery):
        if not is_admin(call): return
        tid = int(call.data.split("_", 2)[2])
        bot.answer_callback_query(call.id, "⚠️ قيد التطوير")
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  ✏️  تعديل المهمة          ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⚠️ _ميزة التعديل المباشر قادمة قريباً_\n\n"
            f"حالياً: احذف المهمة `#{tid}` وأنشئ واحدة جديدة.",
            kb_back(f"task_detail_{tid}")
        )

    # ══════════════════════════════════════════
    #  🔔 الإشعارات
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "menu_notifications")
    def cb_notifications(call: CallbackQuery):
        if not is_admin(call): return
        notifs = arun(db_get_unread_notifications())
        if not notifs:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  🔔  الإشعارات            ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"✅ _لا توجد إشعارات جديدة_",
                kb_back("back_main")
            )
            bot.answer_callback_query(call.id)
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  🔔  إشعارات ({len(notifs)})         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        icons = {"reconnect": "🔄", "ban": "🚫", "new_account": "🆕", "error": "❌"}
        for n in notifs:
            icon = icons.get(n["type"], "🔔")
            lines.append(
                f"{icon} *{n['title']}*\n"
                f"  _{n.get('body','')}_\n"
                f"  {S.format_time(n['created_at'])}"
            )
        arun(db_mark_notifications_read())
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("◀️ رجوع للرئيسية", callback_data="back_main"))
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
        accs      = mgr_get_all_for_task()
        now_str   = datetime.now(TZ).strftime("%H:%M:%S  %d/%m/%Y")
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  🏓  Pong!  — متصل ✅     ║`",
            f"`╚══════════════════════════╝`\n",
            f"🟢 حسابات متصلة: `{connected}`",
            f"📅 جداول نشطة:  `{jobs}`",
            f"🤖 المجدول: `{'🟢 يعمل' if _scheduler.running else '🔴 متوقف'}`",
            f"🕐 `{now_str}`",
        ]
        if accs:
            lines.append(f"\n*الحسابات:*")
            for a in accs[:5]:
                lines.append(f"  🟢 {a['full_name']} `(#{a['id']})`")
        bot.reply_to(msg, "\n".join(lines), parse_mode="Markdown")

    @bot.message_handler(commands=["stats"])
    def cmd_stats(msg: Message):
        if not is_admin(msg): return
        stats = arun(db_get_stats())
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  📊  الإحصائيات           ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"👤 حسابات: `{stats['active_accounts']}/{stats['total_accounts']}`\n"
            f"🟢 متصل:   `{mgr_count()}`\n"
            f"⚙️ مهام:   `{stats['total_tasks']}`\n"
            f"📅 جداول:  `{stats['total_schedules']}`\n\n"
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
                f"*الاستخدام:*\n`/send <acc\\_id> <target> <رسالة>`\n\n"
                f"*مثال:*\n`/send 1 @mygroup مرحبا كيفك!`",
                parse_mode="Markdown"
            )
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id يجب أن يكون رقمًا", parse_mode="Markdown")
        target  = parts[2]
        content = parts[3]   # ← الإصلاح: كانت parts[3] صحيحة لكن التعليق غلط
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, f"❌ الحساب `{aid}` غير متصل", parse_mode="Markdown")
        m = bot.reply_to(msg, S.loading(f"إرسال إلى {target}"), parse_mode="Markdown")
        ok = arun(actions.send_message(target, content))
        bot.edit_message_text(
            f"✅ *تم الإرسال بنجاح!*\n\n"
            f"🎯 `{target}`\n"
            f"📱 الحساب: `{aid}`\n"
            f"💬 _{content[:60]}{'...' if len(content)>60 else ''}_"
            if ok else
            f"❌ *فشل الإرسال!*\n\n🎯 `{target}`\n_تحقق من الهدف_",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["broadcast"])
    def cmd_broadcast(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split(maxsplit=2)
        if len(parts) < 3:
            return bot.reply_to(msg,
                f"*الاستخدام:*\n`/broadcast <target> <رسالة>`\n\n"
                f"_يُرسل من جميع الحسابات المتصلة_\n\n"
                f"*مثال:*\n`/broadcast @mygroup أهلاً بالجميع!`",
                parse_mode="Markdown"
            )
        target, text = parts[1], parts[2]
        ids = mgr_all_connected()
        if not ids: return bot.reply_to(msg, "❌ *لا توجد حسابات متصلة*", parse_mode="Markdown")
        m = bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  📡  بث جماعي            ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🎯 `{target}`\n"
            f"👤 {len(ids)} حساب\n\n"
            f"⏳ _جاري الإرسال..._",
            parse_mode="Markdown"
        )
        ok = fail = 0
        for aid in ids:
            actions = mgr_actions(aid)
            if actions:
                r = arun(actions.send_message(target, text))
                if r: ok += 1
                else: fail += 1
        total = ok + fail
        bar = S.progress_bar(ok, total)
        pct = round(ok / total * 100) if total > 0 else 0
        bot.edit_message_text(
            f"`╔══════════════════════════╗`\n"
            f"`║  📡  نتيجة البث           ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🎯 `{target}`\n\n"
            f"✅ نجح: `{ok}` | ❌ فشل: `{fail}`\n"
            f"`{bar}` `{pct}%`",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["join"])
    def cmd_join(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 3:
            return bot.reply_to(msg,
                f"*الاستخدام:*\n`/join <acc\\_id> <target>`\n\n"
                f"*أمثلة:*\n"
                f"`/join 1 @groupname`\n"
                f"`/join 1 t.me/+xxxxx`\n"
                f"`/join 1 -1001234567890`",
                parse_mode="Markdown"
            )
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id يجب أن يكون رقمًا", parse_mode="Markdown")
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, f"❌ الحساب `{aid}` غير متصل", parse_mode="Markdown")
        target = parts[2]
        m = bot.reply_to(msg, S.loading(f"انضمام إلى {target}"), parse_mode="Markdown")
        ok = arun(actions.join_group(target))
        bot.edit_message_text(
            f"✅ *انضم بنجاح!*\n\n🔗 `{target}`\n📱 الحساب: `{aid}`"
            if ok else
            f"❌ *فشل الانضمام!*\n\n🔗 `{target}`\n_تحقق من الرابط أو الـ username_",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["leave"])
    def cmd_leave(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 3:
            return bot.reply_to(msg,
                f"*الاستخدام:*\n`/leave <acc\\_id> <target>`\n\n"
                f"*أمثلة:*\n"
                f"`/leave 1 @groupname`\n"
                f"`/leave 1 -1001234567890`\n"
                f"`/leave 1 t.me/+xxxxx`",
                parse_mode="Markdown"
            )
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id يجب أن يكون رقمًا", parse_mode="Markdown")
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, f"❌ الحساب `{aid}` غير متصل", parse_mode="Markdown")
        target = parts[2]
        m = bot.reply_to(msg, S.loading(f"مغادرة {target}"), parse_mode="Markdown")
        ok = arun(actions.leave_group(target))
        bot.edit_message_text(
            f"✅ *غادر بنجاح!*\n\n🚪 `{target}`\n📱 الحساب: `{aid}`"
            if ok else
            f"❌ *فشلت المغادرة!*\n\n🚪 `{target}`\n_تأكد أن الحساب موجود في الجروب_",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["dialogs"])
    def cmd_dialogs(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2:
            return bot.reply_to(msg, "الاستخدام: `/dialogs <acc\\_id>`", parse_mode="Markdown")
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ acc\\_id يجب أن يكون رقمًا", parse_mode="Markdown")
        actions = mgr_actions(aid)
        if not actions: return bot.reply_to(msg, "❌ الحساب غير متصل")
        m = bot.reply_to(msg, S.loading("تحميل المحادثات"), parse_mode="Markdown")
        dialogs = arun(actions.get_dialogs(20))
        if not dialogs:
            bot.edit_message_text("📭 _لا توجد محادثات_", m.chat.id, m.message_id, parse_mode="Markdown")
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  💬  محادثات #{aid}         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for d in dialogs:
            name  = getattr(d.entity,"title",None) or getattr(d.entity,"first_name","؟")
            uname = f"  @{d.entity.username}" if getattr(d.entity,"username",None) else ""
            lines.append(f"• `{d.entity.id}` *{name}*{uname}")
        bot.edit_message_text("\n".join(lines), m.chat.id, m.message_id, parse_mode="Markdown")

    @bot.message_handler(commands=["runtask"])
    def cmd_runtask(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2:
            return bot.reply_to(msg, "الاستخدام: `/runtask <id>`", parse_mode="Markdown")
        try: tid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ ID غير صحيح")
        task = arun(db_get_task(tid))
        if not task: return bot.reply_to(msg, f"❌ المهمة `{tid}` غير موجودة", parse_mode="Markdown")
        actions = mgr_actions(task["account_id"])
        if not actions: return bot.reply_to(msg, "❌ الحساب غير متصل")
        m = bot.reply_to(msg, S.loading(f"تنفيذ المهمة {tid}"), parse_mode="Markdown")
        async def _r():
            t = task["task_type"]
            if t == "send_message": return await actions.send_message(task["target"], task["content"] or "", task["parse_mode"])
            elif t == "join_group":  return await actions.join_group(task["target"])
            elif t == "leave_group": return await actions.leave_group(task["target"])
            return False
        ok = arun(_r())
        icon = S.task_type_icon(task["task_type"])
        bot.edit_message_text(
            f"✅ *المهمة `#{tid}` نُفّذت!*\n{icon} `{task['target']}`"
            if ok else
            f"❌ *فشلت المهمة `#{tid}`*\n{icon} `{task['target']}`",
            m.chat.id, m.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["delaccount"])
    def cmd_delacc(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2:
            return bot.reply_to(msg, "الاستخدام: `/delaccount <id>`", parse_mode="Markdown")
        try: aid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ ID غير صحيح")
        arun(mgr_remove(aid))
        bot.reply_to(msg,
            f"✅ *تم حذف الحساب `#{aid}` بنجاح*",
            parse_mode="Markdown"
        )

    @bot.message_handler(commands=["deltask"])
    def cmd_deltask(msg: Message):
        if not is_admin(msg): return
        parts = msg.text.split()
        if len(parts) < 2:
            return bot.reply_to(msg, "الاستخدام: `/deltask <id>`", parse_mode="Markdown")
        try: tid = int(parts[1])
        except ValueError: return bot.reply_to(msg, "❌ ID غير صحيح")
        arun(db_delete_task(tid))
        bot.reply_to(msg,
            f"✅ *تم حذف المهمة `#{tid}` بنجاح*",
            parse_mode="Markdown"
        )

    @bot.message_handler(commands=["update"])
    def cmd_update(msg: Message):
        if not is_admin(msg): return
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔄  تحديث البوت          ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📤 أرسل ملف `.py` الجديد الآن\n\n"
            f"_سيتم استبدال الملف الحالي وإعادة التشغيل تلقائياً_",
            parse_mode="Markdown"
        )
        set_state(msg.from_user.id, {"step": "awaiting_update_file"})

    @bot.message_handler(
        content_types=["document"],
        func=lambda m: in_state(m.from_user.id, "awaiting_update_file")
    )
    def handle_update_file(msg: Message):
        if not is_admin(msg): return
        uid = msg.from_user.id
        doc = msg.document
        if not (doc.file_name or "").endswith(".py"):
            bot.reply_to(msg, "❌ يجب أن يكون الملف بصيغة `.py`", parse_mode="Markdown")
            return
        sent = bot.reply_to(msg, "⏳ _جاري تحميل الملف..._", parse_mode="Markdown")
        try:
            file_info = bot.get_file(doc.file_id)
            file_data = bot.download_file(file_info.file_path)
            try:
                compile(file_data, doc.file_name, "exec")
            except SyntaxError as e:
                clear_state(uid)
                bot.edit_message_text(
                    f"❌ *خطأ في الكود!*\n\n`{e}`\n\n_الملف القديم محتفظ به_",
                    sent.chat.id, sent.message_id, parse_mode="Markdown"
                )
                return
            import shutil
            current_file = Path(__file__).resolve()
            backup_file  = current_file.with_suffix(".py.bak")
            shutil.copy2(current_file, backup_file)
            current_file.write_bytes(file_data)
            clear_state(uid)
            bot.edit_message_text(
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  تم التحديث!          ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📁 `{doc.file_name}` — `{len(file_data):,}` byte\n"
                f"💾 نسخة احتياطية: `{backup_file.name}`\n\n"
                f"🔄 *جاري إعادة التشغيل...*",
                sent.chat.id, sent.message_id, parse_mode="Markdown"
            )
            logger.info(f"✅ تحديث من {msg.from_user.id} — إعادة تشغيل...")
            import time as _t
            def _restart():
                _t.sleep(1.5)
                try: _scheduler.shutdown(wait=False)
                except Exception: pass
                os.execv(sys.executable, [sys.executable] + sys.argv)
            threading.Thread(target=_restart, daemon=True).start()
        except Exception as e:
            clear_state(uid)
            logger.error(f"فشل التحديث: {e}")
            bot.edit_message_text(
                f"❌ *فشل التحديث*\n\n`{e}`",
                sent.chat.id, sent.message_id, parse_mode="Markdown"
            )

    @bot.message_handler(commands=["help"])
    def cmd_help(msg: Message):
        if not is_admin(msg): return
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  📋  دليل الأوامر         ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"*👤 الحسابات:*\n"
            f"  `/addaccount` — إضافة حساب بالهاتف\n"
            f"  `/addsession <key>` — إضافة بـ Session String مباشرة 🔑\n"
            f"  `/delaccount <id>` — حذف حساب\n\n"
            f"*⚙️ المهام:*\n"
            f"  `/addtask` — إنشاء مهمة جديدة\n"
            f"  `/runtask <id>` — تشغيل فوري\n"
            f"  `/deltask <id>` — حذف مهمة\n\n"
            f"*📤 إرسال سريع:*\n"
            f"  `/send <acc> <target> <msg>` — إرسال رسالة\n"
            f"  `/broadcast <target> <msg>` — بث جماعي\n\n"
            f"*👥 الجروبات:*\n"
            f"  `/join <acc> <target>` — انضمام\n"
            f"  `/leave <acc> <target>` — مغادرة\n"
            f"  `/dialogs <acc>` — عرض المحادثات\n\n"
            f"*🤝 التواصل:*\n"
            f"  زر *كلّم صاحب* من القائمة الرئيسية\n"
            f"  أو استخدم `/send <acc> @username <msg>`\n\n"
            f"*📊 النظام:*\n"
            f"  `/stats` — إحصائيات\n"
            f"  `/ping` — اختبار الاتصال\n"
            f"  `/menu` — القائمة الرئيسية\n"
            f"  `/restart` — إعادة تشغيل البوت 🔁\n\n"
            f"*🔑 Session Strings:*\n"
            f"  `/showsessions` — عرض الجلسات\n"
            f"  `/reloadsessions` — إعادة تحميل",
            parse_mode="Markdown",
            reply_markup=kb_main_menu()
        )

    @bot.message_handler(commands=["setcommands"])
    def cmd_setcommands(msg: Message):
        if not is_admin(msg): return
        bot.set_my_commands([
            BotCommand("start",          "🏠 القائمة الرئيسية"),
            BotCommand("menu",           "🎛️ لوحة التحكم"),
            BotCommand("stats",          "📊 الإحصائيات"),
            BotCommand("ping",           "🏓 اختبار الاتصال"),
            BotCommand("addaccount",     "👤 إضافة حساب بهاتف"),
            BotCommand("addsession",     "🔑 إضافة حساب بـ Session String"),
            BotCommand("addtask",        "⚙️ إضافة مهمة"),
            BotCommand("runtask",        "▶️ تشغيل مهمة"),
            BotCommand("send",           "📤 إرسال رسالة"),
            BotCommand("broadcast",      "📡 بث جماعي"),
            BotCommand("join",           "🔗 انضمام لجروب"),
            BotCommand("leave",          "🚪 مغادرة جروب"),
            BotCommand("dialogs",        "💬 عرض المحادثات"),
            BotCommand("showsessions",   "🔑 عرض Session Strings"),
            BotCommand("reloadsessions", "🔄 إعادة تحميل الجلسات"),
            BotCommand("restart",        "🔁 إعادة تشغيل البوت"),
            BotCommand("help",           "❓ المساعدة"),
        ])
        bot.reply_to(msg, "✅ *تم تعيين الأوامر بنجاح!*", parse_mode="Markdown")

    # ══════════════════════════════════════════
    #  👥 قائمة الجروبات والأصدقاء
    # ══════════════════════════════════════════

    def kb_friends_groups_menu() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🤝 أصدقائي",        callback_data="fg_friends"),
            InlineKeyboardButton("👥 جروباتي",         callback_data="fg_groups"),
            InlineKeyboardButton("📡 بث لعدة جروبات",  callback_data="fg_multi_broadcast"),
            InlineKeyboardButton("🔁 رسالة متكررة",    callback_data="fg_auto_msg"),
            InlineKeyboardButton("◀️ رجوع",             callback_data="back_main"),
        )
        return kb

    @bot.callback_query_handler(func=lambda c: c.data == "menu_social")
    def cb_menu_social(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  👥  الأصدقاء والجروبات  ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🟢 حسابات متصلة: `{len(accounts)}`\n\n"
            f"_اختر ما تريد 👇_"
        )
        edit_or_send(call, text, kb_friends_groups_menu())
        bot.answer_callback_query(call.id)

    # ── عرض الأصدقاء ──

    @bot.callback_query_handler(func=lambda c: c.data == "fg_friends")
    def cb_fg_friends(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call, "❌ *لا توجد حسابات متصلة*", kb_back("menu_social"))
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step": "fg_friends_acc"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"fg_friends_acc_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🤝  قائمة الأصدقاء      ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"اختر الحساب لعرض جهات اتصاله 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("fg_friends_acc_"))
    def cb_fg_friends_acc(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_", 3)[3])
        actions = mgr_actions(aid)
        if not actions:
            bot.answer_callback_query(call.id, "❌ الحساب غير متصل")
            return
        bot.answer_callback_query(call.id, "⏳ جاري تحميل الأصدقاء...")
        dialogs = arun(actions.get_dialogs(50))
        # فلترة: الأشخاص فقط (User)
        from telethon.tl.types import User as TLUser
        friends = [d for d in dialogs if isinstance(d.entity, TLUser) and not getattr(d.entity, "bot", False)]
        if not friends:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  🤝  الأصدقاء             ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد محادثات شخصية_",
                kb_back("fg_friends")
            )
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  🤝  أصدقائي ({len(friends)})         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        kb = InlineKeyboardMarkup(row_width=2)
        for d in friends[:20]:
            name  = f"{d.entity.first_name or ''} {d.entity.last_name or ''}".strip() or "؟"
            uname = f"@{d.entity.username}" if getattr(d.entity, "username", None) else f"id:{d.entity.id}"
            lines.append(f"👤 *{name}*  `{uname}`")
            btn_data = f"fg_dm_{aid}_{d.entity.username or d.entity.id}"
            kb.add(InlineKeyboardButton(
                f"💬 {name[:18]}",
                callback_data=btn_data[:64]
            ))
        kb.add(InlineKeyboardButton("◀️ رجوع", callback_data="fg_friends"))
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("fg_dm_"))
    def cb_fg_dm(call: CallbackQuery):
        if not is_admin(call): return
        parts = call.data.split("_", 3)  # fg_dm_AID_TARGET
        aid    = int(parts[2])
        target = parts[3]
        uid    = call.from_user.id
        set_state(uid, {"step": "fg_dm_text", "account_id": aid, "target": target})
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  💬  إرسال رسالة          ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📝 اكتب الرسالة لـ `{target}` 👇\n\n"
            f"_أو اضغط إلغاء_",
            InlineKeyboardMarkup().add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "fg_dm_text"))
    def fg_dm_text(msg: Message):
        uid    = msg.from_user.id
        state  = _states.get(uid, {})
        aid    = state.get("account_id")
        target = state.get("target")
        text   = msg.text
        clear_state(uid)
        actions = mgr_actions(aid)
        if not actions:
            return bot.send_message(msg.chat.id, "❌ *الحساب غير متصل*", parse_mode="Markdown")
        m2 = bot.send_message(msg.chat.id, S.loading(f"إرسال إلى {target}"), parse_mode="Markdown")
        ok = arun(actions.send_message(target, text))
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🤝 الأصدقاء", callback_data="fg_friends"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="back_main"),
        )
        result = (
            f"✅ *تم الإرسال!*\n\n👤 إلى: `{target}`"
        ) if ok else (
            f"❌ *فشل الإرسال!*\n\n👤 الهدف: `{target}`"
        )
        try:
            bot.edit_message_text(result, m2.chat.id, m2.message_id, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            bot.send_message(msg.chat.id, result, parse_mode="Markdown", reply_markup=kb)

    # ── عرض الجروبات ──

    @bot.callback_query_handler(func=lambda c: c.data == "fg_groups")
    def cb_fg_groups(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call, "❌ *لا توجد حسابات متصلة*", kb_back("menu_social"))
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step": "fg_groups_acc"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"fg_groups_acc_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  👥  قائمة الجروبات      ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"اختر الحساب لعرض جروباته 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("fg_groups_acc_"))
    def cb_fg_groups_acc(call: CallbackQuery):
        if not is_admin(call): return
        aid = int(call.data.split("_", 3)[3])
        actions = mgr_actions(aid)
        if not actions:
            bot.answer_callback_query(call.id, "❌ الحساب غير متصل")
            return
        bot.answer_callback_query(call.id, "⏳ جاري تحميل الجروبات...")
        dialogs = arun(actions.get_dialogs(50))
        from telethon.tl.types import Chat, Channel
        groups = [d for d in dialogs if isinstance(d.entity, (Chat, Channel))]
        if not groups:
            edit_or_send(call,
                f"`╔══════════════════════════╗`\n"
                f"`║  👥  الجروبات             ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"📭 _لا توجد جروبات_",
                kb_back("fg_groups")
            )
            return
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  👥  جروباتي ({len(groups)})         ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        kb = InlineKeyboardMarkup(row_width=2)
        for d in groups[:20]:
            title = getattr(d.entity, "title", "؟")
            uname = f"@{d.entity.username}" if getattr(d.entity, "username", None) else f"id:{d.entity.id}"
            lines.append(f"👥 *{title}*  `{uname}`")
            target = d.entity.username or str(d.entity.id)
            btn_data = f"fg_grp_{aid}_{target}"
            kb.add(InlineKeyboardButton(
                f"💬 {title[:18]}",
                callback_data=btn_data[:64]
            ))
        kb.add(InlineKeyboardButton("◀️ رجوع", callback_data="fg_groups"))
        edit_or_send(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("fg_grp_"))
    def cb_fg_grp(call: CallbackQuery):
        if not is_admin(call): return
        parts  = call.data.split("_", 3)  # fg_grp_AID_TARGET
        aid    = int(parts[2])
        target = parts[3]
        uid    = call.from_user.id
        set_state(uid, {"step": "fg_grp_text", "account_id": aid, "target": target})
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  💬  إرسال للجروب         ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📝 اكتب الرسالة للجروب `{target}` 👇",
            InlineKeyboardMarkup().add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "fg_grp_text"))
    def fg_grp_text(msg: Message):
        uid    = msg.from_user.id
        state  = _states.get(uid, {})
        aid    = state.get("account_id")
        target = state.get("target")
        text   = msg.text
        clear_state(uid)
        actions = mgr_actions(aid)
        if not actions:
            return bot.send_message(msg.chat.id, "❌ *الحساب غير متصل*", parse_mode="Markdown")
        m2 = bot.send_message(msg.chat.id, S.loading(f"إرسال إلى {target}"), parse_mode="Markdown")
        ok = arun(actions.send_message(target, text))
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("👥 الجروبات", callback_data="fg_groups"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="back_main"),
        )
        result = f"✅ *تم الإرسال للجروب!*\n\n👥 `{target}`" if ok else f"❌ *فشل الإرسال!*\n\n👥 `{target}`"
        try:
            bot.edit_message_text(result, m2.chat.id, m2.message_id, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            bot.send_message(msg.chat.id, result, parse_mode="Markdown", reply_markup=kb)

    # ══════════════════════════════════════════
    #  📡 بث لأكتر من جروب في نفس الوقت
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "fg_multi_broadcast")
    def cb_fg_multi_broadcast(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call, "❌ *لا توجد حسابات متصلة*", kb_back("menu_social"))
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step": "mb_pick_acc"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"mb_acc_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  📡  بث لعدة جروبات 1/3  ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"👤 اختر الحساب اللي هيبعت منه 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("mb_acc_"))
    def cb_mb_acc(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        aid = int(call.data.split("_", 2)[2])
        _states[uid]["account_id"] = aid
        _states[uid]["step"] = "mb_targets"
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  📡  بث لعدة جروبات 2/3  ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🎯 أرسل الجروبات المستهدفة (كل واحد في سطر):\n\n"
            f"مثال:\n"
            f"`@group1`\n"
            f"`@group2`\n"
            f"`-1001234567890`\n\n"
            f"_أو اضغط إلغاء_",
            InlineKeyboardMarkup().add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "mb_targets"))
    def mb_step_targets(msg: Message):
        uid  = msg.from_user.id
        targets = [t.strip() for t in msg.text.strip().splitlines() if t.strip()]
        if not targets:
            return bot.reply_to(msg, "❌ أرسل جروب واحد على الأقل")
        _states[uid]["targets"] = targets
        _states[uid]["step"]    = "mb_text"
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  📡  بث لعدة جروبات 3/3  ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"✅ *{len(targets)}* جروب محدد\n\n"
            f"📝 اكتب الرسالة اللي هتتبعت 👇",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "mb_text"))
    def mb_step_text(msg: Message):
        uid     = msg.from_user.id
        state   = _states.get(uid, {})
        aid     = state.get("account_id")
        targets = state.get("targets", [])
        text    = msg.text
        clear_state(uid)
        actions = mgr_actions(aid)
        if not actions:
            return bot.send_message(msg.chat.id, "❌ *الحساب غير متصل*", parse_mode="Markdown")
        m2 = bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  📡  جاري الإرسال...      ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⏳ _{len(targets)} جروب..._",
            parse_mode="Markdown"
        )
        results = arun(actions.send_to_many(targets, text))
        ok   = sum(1 for v in results.values() if v == "success")
        fail = len(results) - ok
        bar  = S.progress_bar(ok, len(targets))
        pct  = round(ok / len(targets) * 100) if targets else 0
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  📡  نتيجة البث           ║`",
            f"`╚══════════════════════════╝`\n",
            f"✅ نجح: `{ok}` | ❌ فشل: `{fail}`",
            f"`{bar}` `{pct}%`\n",
        ]
        for t, status in results.items():
            icon = "✅" if status == "success" else "❌"
            lines.append(f"{icon} `{t}`")
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("📡 بث جديد",  callback_data="fg_multi_broadcast"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="back_main"),
        )
        try:
            bot.edit_message_text("\n".join(lines), m2.chat.id, m2.message_id, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="Markdown", reply_markup=kb)

    # ══════════════════════════════════════════
    #  🔁 رسالة متكررة تلقائياً
    # ══════════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "fg_auto_msg")
    def cb_fg_auto_msg(call: CallbackQuery):
        if not is_admin(call): return
        accounts = mgr_get_all_for_task()
        if not accounts:
            edit_or_send(call, "❌ *لا توجد حسابات متصلة*", kb_back("menu_social"))
            bot.answer_callback_query(call.id)
            return
        uid = call.from_user.id
        set_state(uid, {"step": "am_pick_acc"})
        kb = InlineKeyboardMarkup(row_width=1)
        for a in accounts:
            kb.add(InlineKeyboardButton(
                f"🟢 {a.get('full_name') or a['phone']} (#{a['id']})",
                callback_data=f"am_acc_{a['id']}"
            ))
        kb.add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔁  رسالة متكررة 1/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"👤 اختر الحساب اللي هيبعت منه 👇",
            kb
        )
        bot.answer_callback_query(call.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("am_acc_"))
    def cb_am_acc(call: CallbackQuery):
        uid = call.from_user.id
        if not get_state(uid): return
        aid = int(call.data.split("_", 2)[2])
        _states[uid]["account_id"] = aid
        _states[uid]["step"] = "am_target"
        edit_or_send(call,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔁  رسالة متكررة 2/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🎯 أرسل الهدف (جروب أو شخص):\n\n"
            f"• `@group` أو `@username`\n"
            f"• Chat ID: `-1001234567890`",
            InlineKeyboardMarkup().add(InlineKeyboardButton("❌ إلغاء", callback_data="cancel"))
        )
        bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "am_target"))
    def am_step_target(msg: Message):
        uid = msg.from_user.id
        _states[uid]["target"] = msg.text.strip()
        _states[uid]["step"]   = "am_text"
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔁  رسالة متكررة 3/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"📝 اكتب الرسالة 👇",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "am_text"))
    def am_step_text(msg: Message):
        uid = msg.from_user.id
        _states[uid]["content"] = msg.text
        _states[uid]["step"]    = "am_interval"
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔁  رسالة متكررة 4/4    ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⏱️ كل قد إيه هتتبعت الرسالة؟\n\n"
            f"• `minutes=30` — كل 30 دقيقة\n"
            f"• `hours=1` — كل ساعة\n"
            f"• `hours=2 minutes=30` — كل ساعتين ونص\n"
            f"• `days=1` — كل يوم",
            parse_mode="Markdown"
        )

    @bot.message_handler(func=lambda m: in_state(m.from_user.id, "am_interval"))
    def am_step_interval(msg: Message):
        uid   = msg.from_user.id
        state = _states.get(uid, {})
        text  = msg.text.strip()
        try:
            tdata = {}
            for part in text.split():
                if "=" in part:
                    k, v = part.split("=", 1)
                    k = k.strip().lower()
                    if k in ["weeks", "days", "hours", "minutes", "seconds"]:
                        tdata[k] = int(v.strip())
            if not tdata:
                raise ValueError("لم يُتعرف على وحدة زمنية")
        except Exception as e:
            return bot.reply_to(msg, f"❌ {e}\n\nمثال: `minutes=30` أو `hours=1`", parse_mode="Markdown")

        aid     = state.get("account_id")
        target  = state.get("target")
        content = state.get("content")
        clear_state(uid)

        # إنشاء مهمة وجدول آلياً
        try:
            tid = arun(db_add_task(
                f"رسالة متكررة → {target[:15]}",
                aid, "send_message", target, content, created_by=uid
            ))
            sid = arun(sched_add(tid, "interval", tdata, max_runs=-1))
            # تحويل الفترة لنص مقروء
            parts_txt = []
            for k, v in tdata.items():
                parts_txt.append(f"{v} {'ساعة' if k=='hours' else 'يوم' if k=='days' else 'دقيقة' if k=='minutes' else k}")
            interval_txt = " و".join(parts_txt)
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("📅 الجداول",   callback_data="menu_schedules"),
                InlineKeyboardButton("🏠 الرئيسية", callback_data="back_main"),
            )
            bot.send_message(msg.chat.id,
                f"`╔══════════════════════════╗`\n"
                f"`║  ✅  تم إنشاء الجدول!     ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"🎯 الهدف: `{target}`\n"
                f"⏱️ كل: *{interval_txt}*\n"
                f"🔁 التكرار: *لانهائي* ♾️\n"
                f"🆔 المهمة: `{tid}` | الجدول: `{sid}`\n\n"
                f"🚀 _الرسالة ستُرسل تلقائياً الآن!_",
                parse_mode="Markdown", reply_markup=kb
            )
        except Exception as e:
            bot.send_message(msg.chat.id, S.error(str(e)), parse_mode="Markdown")

    # ── تعديل القائمة الرئيسية لإضافة زر الأصدقاء والجروبات ──


    @bot.message_handler(commands=["showsessions"])
    def cmd_showsessions(msg: Message):
        if not is_admin(msg): return
        if not SESSION_ACCOUNTS:
            return bot.reply_to(msg,
                f"`╔══════════════════════════╗`\n"
                f"`║  🔑  Session Strings       ║`\n"
                f"`╚══════════════════════════╝`\n\n"
                f"⚠️ _لا توجد Session Strings في متغيرات البيئة_\n\n"
                f"أضف متغيرات بالشكل:\n`SESSION_1`, `SESSION_2`, ...",
                parse_mode="Markdown"
            )
        lines = [
            f"`╔══════════════════════════╗`",
            f"`║  🔑  Session Strings       ║`",
            f"`╚══════════════════════════╝`\n",
        ]
        for num, ss in SESSION_ACCOUNTS.items():
            client = next((ub for ub in _clients.values() if ub.phone == f"ENV_SESSION_{num}"), None)
            status = "🟢 متصل" if (client and client.is_connected) else "🔴 منفصل"
            name   = client.display_name() if client else "—"
            db_id  = client.account_id if client else "—"
            preview = ss[:8] + "..." + ss[-4:] if len(ss) > 14 else ss
            lines.append(
                f"🔑 *SESSION\\_{num}*\n"
                f"  👤 {name} | {status}\n"
                f"  🆔 `{db_id}` | 🔐 `{preview}`"
            )
        bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="Markdown")

    @bot.message_handler(commands=["reloadsessions"])
    def cmd_reloadsessions(msg: Message):
        if not is_admin(msg): return
        if not SESSION_ACCOUNTS:
            return bot.reply_to(msg,
                "⚠️ _لا توجد SESSION\\_ متغيرات في البيئة_",
                parse_mode="Markdown"
            )
        sent_msg = bot.reply_to(msg,
            f"🔄 *إعادة تحميل {len(SESSION_ACCOUNTS)} Session String...*",
            parse_mode="Markdown"
        )
        to_remove = [aid for aid, ub in list(_clients.items()) if ub.phone.startswith("ENV_SESSION_")]
        for aid in to_remove:
            arun(_clients[aid].disconnect())
            _clients.pop(aid, None)
            _actions.pop(aid, None)
        fresh = load_all_sessions()
        SESSION_ACCOUNTS.clear()
        SESSION_ACCOUNTS.update(fresh)
        results = arun(asyncio.gather(
            *[load_session_from_env(num, ss) for num, ss in SESSION_ACCOUNTS.items()],
            return_exceptions=True
        ))
        ok = sum(1 for r in results if r is True)
        bot.edit_message_text(
            f"`╔══════════════════════════╗`\n"
            f"`║  🔄  إعادة التحميل        ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"🔑 الإجمالي: `{len(SESSION_ACCOUNTS)}`\n"
            f"🟢 متصل:    `{ok}`\n"
            f"🔴 فشل:     `{len(SESSION_ACCOUNTS) - ok}`",
            sent_msg.chat.id, sent_msg.message_id, parse_mode="Markdown"
        )

    @bot.message_handler(commands=["restart"])
    def cmd_restart(msg: Message):
        if not is_admin(msg): return
        bot.send_message(msg.chat.id,
            f"`╔══════════════════════════╗`\n"
            f"`║  🔁  إعادة تشغيل البوت   ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"⏳ _جاري إعادة التشغيل..._",
            parse_mode="Markdown"
        )
        logger.info(f"🔁 إعادة تشغيل بطلب من {msg.from_user.id}")
        def _restart():
            import time as _t
            _t.sleep(1.5)
            try: _scheduler.shutdown(wait=False)
            except Exception: pass
            os.execv(sys.executable, [sys.executable] + sys.argv)
        threading.Thread(target=_restart, daemon=True).start()

    @bot.message_handler(func=lambda m: True)
    def fallback(msg: Message):
        if not is_admin(msg): return
        # تجاهل الرسائل غير المعروفة بصمت


    logger.info("✅ البوت جاهز مع جميع الأوامر")

# ══════════════════════════════════════════════
#  🚀 Main
# ══════════════════════════════════════════════

async def startup():
    logger.info("╔══════════════════════════════════════╗")
    logger.info(f"║  ⚡  TG Manager Pro v{VERSION}         ║")
    logger.info("╚══════════════════════════════════════╝")
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
    jobs = len(sched_jobs())
    if connected:
        acc_lines = "\n".join(
            f"  🟢 {a['full_name']} `(#{a['id']})`{'  🔑' if a.get('is_env') else ''}"
            for a in connected
        )
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  🚀  TG Manager Pro v{VERSION}  ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"✅ *البوت شغّال!*\n"
            f"🕐 `{now}`\n"
            f"📅 جداول نشطة: `{jobs}`\n\n"
            f"*{len(connected)} حساب متصل:*\n"
            f"{acc_lines}"
        )
    else:
        text = (
            f"`╔══════════════════════════╗`\n"
            f"`║  🚀  TG Manager Pro v{VERSION}  ║`\n"
            f"`╚══════════════════════════╝`\n\n"
            f"✅ *البوت شغّال!*\n"
            f"🕐 `{now}`\n\n"
            f"⚠️ _لا توجد حسابات متصلة حالياً_\n"
            f"استخدم قائمة الحسابات لإضافة حساب"
        )
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text, parse_mode="Markdown", reply_markup=kb_main_menu())
            logger.info(f"✅ رسالة البدء أُرسلت لـ {admin_id}")
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
        import time as _time
        # انتظر حتى يتصل البوت
        for _ in range(20):
            _time.sleep(1)
            try:
                bot.get_me()
                logger.info("✅ البوت متصل بـ Telegram API")
                break
            except Exception:
                pass
        # أرسل رسالة البدء لجميع الأدمنز
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
