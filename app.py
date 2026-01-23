import os
import re
import sqlite3
import random
import time
import csv
from datetime import datetime, date
from typing import List, Any, Optional, Dict, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dateutil import parser as dtparser
import requests

# -------------------------------------------------------------
# BASIC CONFIG
# -------------------------------------------------------------
APP_TITLE = "Radom CRM"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(DATA_DIR, "radom_crm.db")
BACKUP_FILE = os.path.join(DATA_DIR, "contacts_backup.csv")

DEFAULT_PASSWORD = "CatJorge"
OTP_TTL_SECONDS = 300  # 5 minutes

APPLICATIONS = sorted(
    [
        "PFAS destruction",
        "CO2 conversion",
        "Waste-to-Energy",
        "NOx production",
        "Hydrogen production",
        "Carbon black production",
        "Mining waste",
        "Reentry",
        "Propulsion",
        "Methane reforming",
        "Communication",
        "Ultrasonic",
        "Nitrification",
        "Surface treatment",
    ]
)

PRODUCTS = ["1 kW", "10 kW", "100 kW", "1 MW"]

PIPELINE = [
    "New",
    "Contacted",
    "Meeting",
    "Quoted",
    "Won",
    "Lost",
    "Nurture",
    "Pending",
    "On hold",
    "Irrelevant",
]

OWNERS = ["", "Velibor", "Liz", "Jovan", "Ian", "Qi", "Kenshin"]

# -------------------------------------------------------------
# dtype-safe numeric helpers
# -------------------------------------------------------------
def safe_int_series(s: pd.Series, default: int = 0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype("int64")


def safe_float_series(s: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype("float64")


# -------------------------------------------------------------
# NOTES IMPORT sanitize + trim email threads
# -------------------------------------------------------------
_EMAIL_THREAD_MARKERS = (
    "\nOn ",
    "\nFrom:",
    "\nSent:",
    "\nSubject:",
    "\nTo:",
    "\nCc:",
)


def sanitize_note_text(v: Any, *, trim_email_threads: bool = True, max_len: int = 4000) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    if trim_email_threads:
        for marker in _EMAIL_THREAD_MARKERS:
            if marker in s:
                s = s.split(marker)[0]
                break
        s = re.split(r"\nOn\s.+\swrote:\s*\n", s, maxsplit=1)[0]

    s = re.sub(r"\n+", " ⏎ ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if max_len and len(s) > max_len:
        s = s[:max_len].rstrip() + "…"
    return s


# -------------------------------------------------------------
# DB
# -------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _ensure_columns(conn: sqlite3.Connection, table: str, required: Dict[str, str]):
    cols = set(_table_cols(conn, table))
    cur = conn.cursor()
    for c, ddl in required.items():
        if c not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {c} {ddl}")
    conn.commit()


def _backfill_unit_price_cents(conn: sqlite3.Connection):
    cols = set(_table_cols(conn, "sales"))
    if "unit_price" in cols and "unit_price_cents" in cols:
        conn.execute(
            """
            UPDATE sales
            SET unit_price_cents = COALESCE(unit_price_cents, CAST(ROUND(unit_price * 100.0) AS INTEGER))
            WHERE unit_price_cents IS NULL OR unit_price_cents = 0
            """
        )
        conn.commit()


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS contacts (
          id INTEGER PRIMARY KEY,
          scan_datetime TEXT,
          first_name TEXT,
          last_name TEXT,
          job_title TEXT,
          company TEXT,
          street TEXT,
          street2 TEXT,
          zip_code TEXT,
          city TEXT,
          state TEXT,
          country TEXT,
          phone TEXT,
          email TEXT,
          website TEXT,
          category TEXT,
          status TEXT DEFAULT 'New',
          owner TEXT,
          last_touch TEXT,
          gender TEXT,
          application TEXT,
          product_interest TEXT,
          photo TEXT,
          profile_url TEXT,
          dedupe_key TEXT
        );

        CREATE TABLE IF NOT EXISTS notes (
          id INTEGER PRIMARY KEY,
          contact_id INTEGER,
          ts TEXT,
          body TEXT,
          next_followup TEXT,
          FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS status_history (
          id INTEGER PRIMARY KEY,
          contact_id INTEGER,
          ts TEXT,
          old_status TEXT,
          new_status TEXT,
          FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS telegram_users (
          username TEXT PRIMARY KEY,
          chat_id INTEGER,
          first_seen TEXT
        );

        CREATE TABLE IF NOT EXISTS sales (
          id INTEGER PRIMARY KEY,
          contact_id INTEGER NOT NULL,
          sold_at TEXT NOT NULL,
          product TEXT NOT NULL,
          qty INTEGER NOT NULL DEFAULT 1,
          unit_price_cents INTEGER NOT NULL DEFAULT 0,
          currency TEXT NOT NULL DEFAULT 'USD',
          note TEXT,
          FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        );
        """
    )

    _ensure_columns(
        conn,
        "contacts",
        {
            "profile_url": "TEXT",
            "photo": "TEXT",
            "owner": "TEXT",
            "last_touch": "TEXT",
            "website": "TEXT",
            "gender": "TEXT",
            "application": "TEXT",
            "product_interest": "TEXT",
            "country": "TEXT",
            "dedupe_key": "TEXT",
        },
    )

    _ensure_columns(
        conn,
        "sales",
        {
            "qty": "INTEGER NOT NULL DEFAULT 1",
            "unit_price_cents": "INTEGER NOT NULL DEFAULT 0",
            "currency": "TEXT NOT NULL DEFAULT 'USD'",
            "note": "TEXT",
        },
    )

    _backfill_unit_price_cents(conn)


# -------------------------------------------------------------
# URL + FLAGS HELPERS
# -------------------------------------------------------------
def _clean_url(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return "https://" + s.lstrip("/")


_COUNTRY_TO_ISO2 = {
    "united states": "US",
    "usa": "US",
    "u.s.a.": "US",
    "us": "US",
    "canada": "CA",
    "mexico": "MX",
    "colombia": "CO",
    "chile": "CL",
    "peru": "PE",
    "brazil": "BR",
    "argentina": "AR",
    "united kingdom": "GB",
    "uk": "GB",
    "england": "GB",
    "germany": "DE",
    "france": "FR",
    "italy": "IT",
    "spain": "ES",
    "netherlands": "NL",
    "belgium": "BE",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "finland": "FI",
    "switzerland": "CH",
    "austria": "AT",
    "poland": "PL",
    "czech republic": "CZ",
    "czechia": "CZ",
    "slovakia": "SK",
    "slovenia": "SI",
    "croatia": "HR",
    "bosnia and herzegovina": "BA",
    "serbia": "RS",
    "romania": "RO",
    "bulgaria": "BG",
    "greece": "GR",
    "turkey": "TR",
    "russia": "RU",
    "ukraine": "UA",
    "israel": "IL",
    "saudi arabia": "SA",
    "uae": "AE",
    "united arab emirates": "AE",
    "qatar": "QA",
    "india": "IN",
    "china": "CN",
    "japan": "JP",
    "south korea": "KR",
    "korea": "KR",
    "taiwan": "TW",
    "singapore": "SG",
    "australia": "AU",
    "new zealand": "NZ",
}


def flag_img(country: Any, size: int = 18) -> str:
    if country is None:
        return ""
    s = str(country).strip()
    if not s:
        return ""
    iso = ""
    if len(s) == 2 and s.isalpha():
        iso = s.upper()
    else:
        iso = _COUNTRY_TO_ISO2.get(s.lower(), "")
    if not iso:
        return ""
    return (
        f"<img src='https://flagcdn.com/{iso.lower()}.svg' width='{size}' "
        f"style='vertical-align:middle;border-radius:2px;margin-left:6px;'/>"
    )


# -------------------------------------------------------------
# DEDUPE KEY
# -------------------------------------------------------------
def _norm_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_company(v: Any) -> str:
    s = _norm_text(v)
    if not s:
        return ""
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    s = re.sub(r"\b(inc|incorporated|llc|ltd|co|corp|corporation|company|gmbh|sarl|sa|plc)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_email(v: Any) -> str:
    s = _norm_text(v)
    if "@" not in s:
        return ""
    return s


def _norm_profile(v: Any) -> str:
    s = _clean_url(v).strip().lower()
    if not s:
        return ""
    s = re.sub(r"[?#].*$", "", s)
    s = s.rstrip("/")
    return s


def compute_dedupe_key(first: Any, last: Any, company: Any, email: Any, profile_url: Any) -> str:
    em = _norm_email(email)
    if em:
        return f"email:{em}"
    pr = _norm_profile(profile_url)
    if pr:
        return f"profile:{pr}"
    fn = _norm_text(first)
    ln = _norm_text(last)
    co = _norm_company(company)
    if fn or ln or co:
        return f"nameco:{fn}|{ln}|{co}"
    return ""


# -------------------------------------------------------------
# TELEGRAM OTP
# -------------------------------------------------------------
def _tg_token() -> str:
    try:
        return str(st.secrets.get("TELEGRAM_BOT_TOKEN", "")).strip()
    except Exception:
        return ""


def _tg_api(method: str) -> str:
    token = _tg_token()
    return f"https://api.telegram.org/bot{token}/{method}"


def telegram_get_me() -> Tuple[int, str]:
    token = _tg_token()
    if not token:
        return 0, "Missing TELEGRAM_BOT_TOKEN in secrets."
    try:
        r = requests.get(_tg_api("getMe"), timeout=10)
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


def telegram_get_updates() -> Tuple[int, str]:
    token = _tg_token()
    if not token:
        return 0, "Missing TELEGRAM_BOT_TOKEN in secrets."
    try:
        r = requests.get(_tg_api("getUpdates"), params={"limit": 50}, timeout=15)
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


def telegram_find_chat_id_by_username(username: str) -> Optional[int]:
    username = (username or "").strip().lstrip("@")
    if not username:
        return None
    token = _tg_token()
    if not token:
        return None

    cache: Dict[str, int] = st.session_state.setdefault("tg_user_cache", {})
    if username.lower() in cache:
        return cache[username.lower()]

    try:
        conn = get_conn()
        init_db(conn)
        row = conn.execute(
            "SELECT chat_id FROM telegram_users WHERE lower(username)=?",
            (username.lower(),),
        ).fetchone()
        if row and row[0]:
            cache[username.lower()] = int(row[0])
            return int(row[0])
    except Exception:
        pass

    try:
        resp = requests.get(_tg_api("getUpdates"), params={"limit": 100}, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data.get("ok"):
            return None

        best: Optional[int] = None
        for upd in data.get("result", []):
            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            chat = msg.get("chat") or {}
            frm = msg.get("from") or {}
            u1 = (frm.get("username") or "").strip().lstrip("@")
            u2 = (chat.get("username") or "").strip().lstrip("@")
            if u1.lower() == username.lower() or u2.lower() == username.lower():
                if chat.get("type") == "private" and chat.get("id") is not None:
                    best = int(chat["id"])

        if best is not None:
            cache[username.lower()] = best
            try:
                conn = get_conn()
                init_db(conn)
                conn.execute(
                    "INSERT OR REPLACE INTO telegram_users(username, chat_id, first_seen) VALUES (?,?,?)",
                    (username.lower(), int(best), datetime.utcnow().isoformat()),
                )
                conn.commit()
            except Exception:
                pass
            return best

    except Exception:
        return None
    return None


def telegram_send_message(chat_id: int, text: str) -> Tuple[bool, str]:
    token = _tg_token()
    if not token:
        return False, "Missing TELEGRAM_BOT_TOKEN"
    try:
        r = requests.post(
            _tg_api("sendMessage"),
            json={"chat_id": int(chat_id), "text": text},
            timeout=10,
        )
        if r.status_code == 200:
            return True, r.text
        return False, f"Status {r.status_code}: {r.text}"
    except Exception as e:
        return False, str(e)


def check_login_two_factor_telegram():
    try:
        expected = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)
    except Exception:
        expected = DEFAULT_PASSWORD

    ss = st.session_state
    ss.setdefault("auth_pw_ok", False)
    ss.setdefault("authed", False)

    if ss["authed"]:
        return

    st.sidebar.header("🔐 Login")

    # username FIRST, then password
    tg_user = st.sidebar.text_input("Telegram username (without @)", key="login_tg_user").strip().lstrip("@")
    pwd = st.sidebar.text_input("Password", type="password", key="login_pwd")

    if not ss["auth_pw_ok"]:
        if st.sidebar.button("Continue"):
            if not tg_user:
                st.sidebar.error("Please enter your Telegram username.")
                st.stop()
            if pwd != expected:
                st.sidebar.error("Wrong password")
                st.stop()

            ss["auth_pw_ok"] = True
            ss["login_username"] = tg_user

            code = f"{random.randint(0, 999999):06d}"
            ss["otp_code"] = code
            ss["otp_time"] = int(time.time())
            ss["otp_delivery_ok"] = False
            ss["otp_delivery_msg"] = ""

            chat_id = telegram_find_chat_id_by_username(tg_user)
            if chat_id is None:
                ss["otp_delivery_ok"] = False
                ss["otp_delivery_msg"] = (
                    "Could not detect your Telegram chat. Open Telegram, search for the bot, press Start, "
                    "send any message (e.g., 'hi'), then try again."
                )
            else:
                ok, msg = telegram_send_message(
                    chat_id,
                    f"Radom CRM login code: {code} (valid {OTP_TTL_SECONDS//60} min)",
                )
                ss["otp_delivery_ok"] = bool(ok)
                ss["otp_delivery_msg"] = msg if ok else "Failed to send Telegram message."

            st.rerun()
        st.stop()

    if "otp_time" in ss and int(time.time()) - ss["otp_time"] > OTP_TTL_SECONDS:
        for k in ("auth_pw_ok", "otp_code", "otp_time", "otp_delivery_ok", "otp_delivery_msg", "login_username"):
            ss.pop(k, None)
        st.sidebar.error("Code expired. Please start over.")
        st.stop()

    st.sidebar.caption("Enter the 6-digit code sent to your Telegram private chat with the bot.")
    code_in = st.sidebar.text_input("Enter 6-digit code", max_chars=6)

    colv1, colv2 = st.sidebar.columns(2)
    with colv1:
        if st.sidebar.button("Verify"):
            if code_in.strip() == ss.get("otp_code", ""):
                ss["authed"] = True
                for k in ("auth_pw_ok", "otp_code", "otp_time", "otp_delivery_ok", "otp_delivery_msg", "login_username"):
                    ss.pop(k, None)
                st.rerun()
            else:
                st.sidebar.error("Incorrect code")
                st.stop()

    with colv2:
        if st.sidebar.button("Start over"):
            for k in ("auth_pw_ok", "otp_code", "otp_time", "otp_delivery_ok", "otp_delivery_msg", "login_username"):
                ss.pop(k, None)
            st.rerun()

    with st.sidebar.expander("Troubleshooting"):
        # ✅ SECURITY FIX: never display OTP code in UI (no fallback code)
        if not ss.get("otp_delivery_ok", False):
            st.write(ss.get("otp_delivery_msg") or "Telegram delivery failed.")
            st.info("No fallback code is shown for security. Please fix Telegram delivery and try again.")

        st.write("**Bot health check**")
        if st.button("Test getMe"):
            status, txt = telegram_get_me()
            st.write(f"Status: {status}")
            st.code(txt)

        if st.button("Show getUpdates (recent)"):
            status, txt = telegram_get_updates()
            st.write(f"Status: {status}")
            st.code(txt)

    st.stop()


# -------------------------------------------------------------
# BACKUP / RESTORE
# -------------------------------------------------------------
def backup_contacts(conn: sqlite3.Connection):
    df = pd.read_sql_query("SELECT * FROM contacts", conn)
    if not df.empty:
        os.makedirs(DATA_DIR, exist_ok=True)
        df.to_csv(BACKUP_FILE, index=False)


def restore_from_backup_if_empty(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM contacts")
    n = cur.fetchone()[0]
    if n == 0 and os.path.exists(BACKUP_FILE):
        try:
            df = pd.read_csv(BACKUP_FILE)
            if not df.empty:
                upsert_contacts(conn, df)
        except Exception as e:
            print(f"Backup restore failed: {e}")


# -------------------------------------------------------------
# DEDUPE
# -------------------------------------------------------------
def ensure_dedupe_index(conn: sqlite3.Connection):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_dedupe_key
            ON contacts(dedupe_key)
            WHERE dedupe_key IS NOT NULL AND TRIM(dedupe_key) <> ''
            """
        )
        conn.commit()
    except Exception:
        pass


def dedupe_database(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    try:
        cur.execute("DROP INDEX IF EXISTS idx_contacts_dedupe_key")
    except Exception:
        pass

    cur.execute("UPDATE contacts SET dedupe_key=NULL")
    conn.commit()

    rows = cur.execute("SELECT id, first_name, last_name, company, email, profile_url FROM contacts").fetchall()
    for (cid, first, last, company, email, profile_url) in rows:
        key = compute_dedupe_key(first, last, company, email, profile_url)
        cur.execute("UPDATE contacts SET dedupe_key=? WHERE id=?", (key or None, cid))
    conn.commit()

    dup_keys = cur.execute(
        """
        SELECT dedupe_key
        FROM contacts
        WHERE dedupe_key IS NOT NULL AND TRIM(dedupe_key) <> ''
        GROUP BY dedupe_key
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    deleted = 0
    for (k,) in dup_keys:
        ids = [r[0] for r in cur.execute("SELECT id FROM contacts WHERE dedupe_key=? ORDER BY id ASC", (k,)).fetchall()]
        if len(ids) <= 1:
            continue

        winner = ids[0]
        losers = ids[1:]

        for lose_id in losers:
            cur.execute("UPDATE notes SET contact_id=? WHERE contact_id=?", (winner, lose_id))
            cur.execute("UPDATE status_history SET contact_id=? WHERE contact_id=?", (winner, lose_id))
            cur.execute("UPDATE sales SET contact_id=? WHERE contact_id=?", (winner, lose_id))

        cur.execute("DELETE FROM contacts WHERE id IN (" + ",".join("?" for _ in losers) + ")", losers)
        deleted += len(losers)

    conn.commit()
    ensure_dedupe_index(conn)
    backup_contacts(conn)
    return deleted


# -------------------------------------------------------------
# IMPORT / NORMALIZATION
# -------------------------------------------------------------
COLMAP = {
    "scan date/time": "scan_datetime",
    "scan_datetime": "scan_datetime",
    "first name": "first_name",
    "first_name": "first_name",
    "last name": "last_name",
    "last_name": "last_name",
    "job title": "job_title",
    "job_title": "job_title",
    "company": "company",
    "street": "street",
    "street (line 2)": "street2",
    "street2": "street2",
    "zip code": "zip_code",
    "zip_code": "zip_code",
    "city": "city",
    "state/province": "state",
    "state": "state",
    "country": "country",
    "phone": "phone",
    "email": "email",
    "notes": "notes",
    "comment": "notes",
    "comments": "notes",
    "email comments": "notes",
    "email responses": "notes",
    "website": "website",
    "gender": "gender",
    "application": "application",
    "product interest": "product_interest",
    "product_interest": "product_interest",
    "product_type_interest": "product_interest",
    "status": "status",
    "pipeline": "status",
    "stage": "status",
    "photo": "photo",
    "owner": "owner",
    "last_touch": "last_touch",
    "linkedin": "profile_url",
    "linkedin url": "profile_url",
    "linkedin_url": "profile_url",
    "linkedin profile": "profile_url",
    "linkedin profile url": "profile_url",
    "profile": "profile_url",
    "profile url": "profile_url",
    "profile link": "profile_url",
    "profile_url": "profile_url",
    # If you import from an exported CSV that includes sales aggregates
    "sold_qty": "sold_qty",
    "sold_revenue_cents": "sold_revenue_cents",
    "sold_revenue_usd": "sold_revenue_usd",
    "sales_lines": "sales_lines",
    "first_sold_at": "first_sold_at",
    "last_sold_at": "last_sold_at",
}

EXPECTED = [
    "scan_datetime",
    "first_name",
    "last_name",
    "job_title",
    "company",
    "street",
    "street2",
    "zip_code",
    "city",
    "state",
    "country",
    "phone",
    "email",
    "website",
    "notes",
    "gender",
    "application",
    "product_interest",
    "status",
    "owner",
    "last_touch",
    "photo",
    "profile_url",
]

STUDENT_PAT = re.compile(r"\b(phd|ph\.d|student|undergrad|graduate)\b", re.I)
PROF_PAT = re.compile(r"\b(assistant|associate|full)?\s*professor\b|department chair", re.I)
IND_PAT = re.compile(r"\b(director|manager|engineer|scientist|vp|founder|ceo|cto|lead|principal)\b", re.I)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {c: COLMAP.get(str(c).strip().lower(), str(c).strip().lower()) for c in df.columns}
    df = df.rename(columns=new_cols)
    for c in EXPECTED:
        if c not in df.columns:
            df[c] = None
    return df


def infer_category(row: pd.Series) -> str:
    title = (row.get("job_title") or "")
    email = (row.get("email") or "")
    domain = email.split("@")[-1].lower() if "@" in email else ""
    if STUDENT_PAT.search(title):
        return "PhD/Student"
    if PROF_PAT.search(title):
        return "Professor/Academic"
    if any(x in domain for x in (".edu", ".ac.", "ac.uk", ".edu.", ".ac.nz", ".ac.in")):
        return "Academic"
    if IND_PAT.search(title):
        return "Industry"
    return "Other"


def parse_dt(v) -> Optional[str]:
    if v is None or str(v).strip() == "" or pd.isna(v):
        return None
    try:
        return dtparser.parse(str(v)).isoformat()
    except Exception:
        return str(v)


def normalize_status(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    for p in PIPELINE:
        if s == p.lower():
            return p
    synonyms = {
        "new lead": "New",
        "contact": "Contacted",
        "meeting scheduled": "Meeting",
        "quote": "Quoted",
        "won deal": "Won",
        "lost deal": "Lost",
        "follow up": "Nurture",
        "follow-up": "Nurture",
    }
    if s in synonyms:
        return synonyms[s]
    return None


def normalize_application(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    for app in APPLICATIONS:
        if s == app.lower():
            return app
    if "pfas" in s:
        return "PFAS destruction"
    if "co2" in s or "carbon dioxide" in s:
        return "CO2 conversion"
    if "waste" in s or "gasification" in s or "rdf" in s:
        return "Waste-to-Energy"
    if "nox" in s or "nitric" in s or "nitrate" in s:
        return "NOx production"
    if "nitrification" in s:
        return "Nitrification"
    if "hydrogen" in s or "h2" in s:
        return "Hydrogen production"
    if "carbon black" in s or "soot" in s:
        return "Carbon black production"
    if "mining" in s or "tailings" in s:
        return "Mining waste"
    if "reentry" in s or "re-entry" in s:
        return "Reentry"
    if "propulsion" in s or "rocket" in s or "thruster" in s:
        return "Propulsion"
    if "methane" in s or "reforming" in s or "steam reforming" in s:
        return "Methane reforming"
    if "communication" in s:
        return "Communication"
    if "ultrasonic" in s or "ultrasound" in s:
        return "Ultrasonic"
    if "surface" in s and ("treat" in s or "coating" in s or "modify" in s):
        return "Surface treatment"
    return None


def _fix_header_row_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    cols_lower = [str(c).strip().lower() for c in df.columns]
    if "first_name" in cols_lower or "first name" in cols_lower:
        return df
    if df.empty:
        return df
    first_row = df.iloc[0]
    first_vals = ["" if (isinstance(v, float) and pd.isna(v)) else str(v).strip() for v in first_row]
    first_vals_lower = [v.lower() for v in first_vals]
    known = set(COLMAP.keys()) | set(EXPECTED)
    score = sum(1 for v in first_vals_lower if v in known)
    if score >= 3:
        new_cols = []
        for i, val in enumerate(first_vals_lower):
            new_cols.append(val if val else f"extra_{i}")
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_cols
        for c in list(df.columns):
            if c.startswith("extra_") and df[c].isna().all():
                df = df.drop(columns=[c])
    return df


def load_contacts_file(uploaded_file) -> pd.DataFrame:
    if uploaded_file.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
    return _fix_header_row_if_needed(df)


# -------------------------------------------------------------
# UPSERT (NO DUPLICATES)
# -------------------------------------------------------------
def _find_existing_contact_id(
    cur: sqlite3.Cursor,
    dedupe_key: str,
    email: Optional[str],
    profile_url: Optional[str],
) -> Optional[int]:
    if email:
        row = cur.execute("SELECT id FROM contacts WHERE email=?", (email,)).fetchone()
        if row:
            return int(row[0])
    if profile_url:
        row = cur.execute("SELECT id FROM contacts WHERE lower(profile_url)=?", (profile_url.lower(),)).fetchone()
        if row:
            return int(row[0])
    if dedupe_key:
        row = cur.execute("SELECT id FROM contacts WHERE dedupe_key=?", (dedupe_key,)).fetchone()
        if row:
            return int(row[0])
    return None


def _usd_to_cents(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if s == "":
        return None
    try:
        val = float(s)
        return int(round(val * 100))
    except Exception:
        return None


def _parse_sold_at_to_iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        dt = dtparser.parse(s)
        return dt.date().isoformat()
    except Exception:
        return None


_SALES_LINE_RE = re.compile(
    r"^\s*(\d{4}-\d{2}-\d{2})\s*:\s*(.*?)\s*x(\d+)\s*@\s*\$([\d,]+(?:\.\d+)?)\s*$"
)


def _extract_sales_rows_from_import(r: pd.Series) -> List[Dict[str, Any]]:
    """
    Supports import from your exported CSV where 'sales_lines' looks like:
      2025-12-31: 1 kW x1 @ $35,000 | 2026-01-02: 10 kW x2 @ $40,000
    Fallback: if sold_qty + sold_revenue are present, creates 1 synthetic sales line.
    """
    rows: List[Dict[str, Any]] = []

    sales_lines = str(r.get("sales_lines") or "").strip()
    if sales_lines and sales_lines.lower() != "nan":
        parts = [p.strip() for p in sales_lines.split("|") if p.strip()]
        for p in parts:
            m = _SALES_LINE_RE.match(p)
            if not m:
                continue
            sold_at = m.group(1)
            product = (m.group(2) or "").strip() or "1 kW"
            qty = int(m.group(3))
            unit_price_usd = float(str(m.group(4)).replace(",", ""))
            rows.append(
                {
                    "sold_at": sold_at,
                    "product": product,
                    "qty": max(1, int(qty)),
                    "unit_price_cents": int(round(unit_price_usd * 100)),
                    "note": "Imported from CSV (sales_lines)",
                }
            )

    if rows:
        return rows

    # Fallback: build one synthetic line from sold_qty / sold_revenue
    try:
        sold_qty = int(pd.to_numeric(r.get("sold_qty"), errors="coerce") or 0)
    except Exception:
        sold_qty = 0

    sold_rev_cents = pd.to_numeric(r.get("sold_revenue_cents"), errors="coerce")
    sold_rev_usd = pd.to_numeric(r.get("sold_revenue_usd"), errors="coerce")

    rev_cents = 0
    if sold_rev_cents is not None and not pd.isna(sold_rev_cents):
        rev_cents = int(sold_rev_cents)
    elif sold_rev_usd is not None and not pd.isna(sold_rev_usd):
        rev_cents = int(round(float(sold_rev_usd) * 100))

    if sold_qty > 0 and rev_cents > 0:
        sold_at = _parse_sold_at_to_iso(r.get("first_sold_at")) or _parse_sold_at_to_iso(r.get("last_sold_at")) or None
        if not sold_at:
            sold_at = datetime.utcnow().date().isoformat()

        product = (str(r.get("product_interest") or "").strip() or "1 kW")
        unit_price_cents = int(round(rev_cents / max(1, sold_qty)))

        rows.append(
            {
                "sold_at": sold_at,
                "product": product,
                "qty": max(1, int(sold_qty)),
                "unit_price_cents": int(unit_price_cents),
                "note": "Imported from CSV (sold_qty/revenue fallback)",
            }
        )

    return rows


def _upsert_sales_rows(conn: sqlite3.Connection, contact_id: int, sales_rows: List[Dict[str, Any]]):
    if not sales_rows:
        return
    cur = conn.cursor()
    for sr in sales_rows:
        sold_at = str(sr["sold_at"])[:10]
        product = (sr["product"] or "").strip() or "1 kW"
        qty = int(sr.get("qty") or 1)
        unit_price_cents = int(sr.get("unit_price_cents") or 0)
        note = (sr.get("note") or "").strip() or None

        exists = cur.execute(
            """
            SELECT 1 FROM sales
            WHERE contact_id=? AND sold_at=? AND product=? AND qty=? AND unit_price_cents=?
            """,
            (int(contact_id), sold_at, product, int(qty), int(unit_price_cents)),
        ).fetchone()
        if exists:
            continue

        cur.execute(
            """
            INSERT INTO sales(contact_id, sold_at, product, qty, unit_price_cents, currency, note)
            VALUES (?,?,?,?,?,?,?)
            """,
            (int(contact_id), sold_at, product, int(qty), int(unit_price_cents), "USD", note),
        )
    conn.commit()


def upsert_contacts(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    df = normalize_columns(df).fillna("")
    df["category"] = df.apply(infer_category, axis=1)
    df["scan_datetime"] = df["scan_datetime"].apply(parse_dt)
    df["status_norm"] = df.get("status", "").apply(normalize_status)

    n = 0
    cur = conn.cursor()

    for idx, r in df.iterrows():
        email = (_norm_email(r.get("email")) or None)
        raw_note = r.get("notes")
        note_text = sanitize_note_text(raw_note, trim_email_threads=True)

        scan_dt = r.get("scan_datetime") or None
        first = (r.get("first_name") or "").strip() or None
        last = (r.get("last_name") or "").strip() or None
        job = (r.get("job_title") or "").strip() or None
        company = (r.get("company") or "").strip() or None
        street = (r.get("street") or "").strip() or None
        street2 = (r.get("street2") or "").strip() or None
        zipc = (r.get("zip_code") or "").strip() or None
        city = (r.get("city") or "").strip() or None
        state = (r.get("state") or "").strip() or None
        country = (r.get("country") or "").strip() or None
        phone = str(r.get("phone") or "").strip() or None
        website = _clean_url(r.get("website") or "") or None
        gender = (r.get("gender") or "").strip() or None
        application = normalize_application(r.get("application"))
        product_interest = (r.get("product_interest") or "").strip() or None
        owner = (r.get("owner") or "").strip() or None
        last_touch = (r.get("last_touch") or "").strip() or None
        photo = (r.get("photo") or "").strip() or None
        profile_url = _clean_url(r.get("profile_url") or "") or None

        status_from_file = r.get("status_norm") or None
        dedupe_key = compute_dedupe_key(first, last, company, email, profile_url) or None

        try:
            existing_id = _find_existing_contact_id(cur, dedupe_key or "", email, profile_url)
            existing_status = None
            if existing_id:
                row2 = cur.execute("SELECT status FROM contacts WHERE id=?", (existing_id,)).fetchone()
                existing_status = (row2[0] if row2 else "New") or "New"

            final_status = status_from_file or existing_status or "New"

            if existing_id:
                if (existing_status or "New").strip() != (final_status or "New").strip():
                    cur.execute(
                        "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
                        (
                            existing_id,
                            datetime.utcnow().isoformat(),
                            (existing_status or "New").strip(),
                            (final_status or "New").strip(),
                        ),
                    )

                cur.execute(
                    """
                    UPDATE contacts SET
                      scan_datetime=?,
                      first_name=?,
                      last_name=?,
                      job_title=?,
                      company=?,
                      street=?,
                      street2=?,
                      zip_code=?,
                      city=?,
                      state=?,
                      country=?,
                      phone=?,
                      email=?,
                      website=?,
                      category=?,
                      status=?,
                      owner=?,
                      last_touch=?,
                      gender=?,
                      application=?,
                      product_interest=?,
                      photo=?,
                      profile_url=?,
                      dedupe_key=?
                    WHERE id=?
                    """,
                    (
                        scan_dt,
                        first,
                        last,
                        job,
                        company,
                        street,
                        street2,
                        zipc,
                        city,
                        state,
                        country,
                        phone,
                        email,
                        website,
                        r.get("category") or "Other",
                        final_status,
                        owner,
                        last_touch,
                        gender,
                        application,
                        product_interest,
                        photo,
                        profile_url,
                        dedupe_key,
                        existing_id,
                    ),
                )
                contact_id = existing_id
            else:
                cur.execute(
                    """
                    INSERT INTO contacts (
                      scan_datetime, first_name, last_name, job_title, company, street, street2, zip_code,
                      city, state, country, phone, email, website, category, status, owner, last_touch,
                      gender, application, product_interest, photo, profile_url, dedupe_key
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        scan_dt,
                        first,
                        last,
                        job,
                        company,
                        street,
                        street2,
                        zipc,
                        city,
                        state,
                        country,
                        phone,
                        email,
                        website,
                        r.get("category") or "Other",
                        final_status,
                        owner,
                        last_touch,
                        gender,
                        application,
                        product_interest,
                        photo,
                        profile_url,
                        dedupe_key,
                    ),
                )
                contact_id = cur.lastrowid

            if note_text:
                ts_iso = scan_dt or datetime.utcnow().isoformat()
                cur.execute("SELECT 1 FROM notes WHERE contact_id=? AND body=?", (contact_id, note_text))
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                        (contact_id, ts_iso, note_text, None),
                    )

            sales_rows = _extract_sales_rows_from_import(r)
            if sales_rows:
                _upsert_sales_rows(conn, int(contact_id), sales_rows)

            n += 1

        except sqlite3.Error as e:
            st.error(
                f"Database error on row {idx + 1} "
                f"(email='{email}', name='{(first or '')} {(last or '')}'): {e}"
            )
            continue

    conn.commit()
    backup_contacts(conn)
    ensure_dedupe_index(conn)
    return n


# -------------------------------------------------------------
# QUERIES & NOTES
# -------------------------------------------------------------
def query_contacts(
    conn: sqlite3.Connection,
    q: str,
    cats: List[str],
    stats: List[str],
    state_like: str,
    app_filter: List[str],
    prod_filter: List[str],
) -> pd.DataFrame:
    sql = """
        SELECT *,
               (SELECT MAX(ts) FROM notes n WHERE n.contact_id = c.id) AS last_note_ts
        FROM contacts c
        WHERE 1=1
    """
    params: List[Any] = []

    if q:
        like = f"%{q}%"
        sql += " AND (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ?)"
        params += [like, like, like, like]

    if cats:
        sql += " AND category IN (" + ",".join("?" for _ in cats) + ")"
        params += cats

    if stats:
        sql += " AND status IN (" + ",".join("?" for _ in stats) + ")"
        params += stats

    if state_like:
        sql += " AND state LIKE ?"
        params.append(f"%{state_like}%")

    if app_filter:
        sql += " AND application IN (" + ",".join("?" for _ in app_filter) + ")"
        params += app_filter

    if prod_filter:
        sql += " AND product_interest IN (" + ",".join("?" for _ in prod_filter) + ")"
        params += prod_filter

    return pd.read_sql_query(sql, conn, params=params)


def get_notes(conn: sqlite3.Connection, contact_id: int) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT ts, body, next_followup FROM notes WHERE contact_id=? ORDER BY ts DESC",
        conn,
        params=(contact_id,),
    )


def get_notes_agg(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query("SELECT contact_id, ts, body FROM notes ORDER BY contact_id, ts", conn)
    if df.empty:
        return pd.DataFrame(columns=["contact_id", "notes"])
    grouped = (
        df.groupby("contact_id")["body"]
        .apply(lambda s: " || ".join([str(x).strip() for x in s if str(x).strip() != ""]))
        .reset_index(name="notes")
    )
    return grouped


def update_contact_status(conn: sqlite3.Connection, contact_id: int, new_status: str):
    new_status = (new_status or "New").strip()
    cur = conn.cursor()
    cur.execute("SELECT status FROM contacts WHERE id=?", (contact_id,))
    row = cur.fetchone()
    if not row:
        return
    old_status = (row[0] or "New").strip()
    if old_status == new_status:
        return

    ts_iso = datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
        (contact_id, ts_iso, old_status, new_status),
    )
    cur.execute("UPDATE contacts SET status=?, last_touch=? WHERE id=?", (new_status, ts_iso, contact_id))
    conn.commit()
    backup_contacts(conn)


# -------------------------------------------------------------
# MANUAL CREATE CONTACT (NEW)  ✅
# -------------------------------------------------------------
def create_contact(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """
    Create a new contact record. Returns new contact_id.
    If the contact already exists (by email/profile/dedupe_key), returns existing id.
    """
    first = (data.get("first_name") or "").strip() or None
    last = (data.get("last_name") or "").strip() or None
    company = (data.get("company") or "").strip() or None
    email = _norm_email(data.get("email")) or None
    profile_url = _clean_url(data.get("profile_url") or "") or None

    job = (data.get("job_title") or "").strip() or None
    phone = (
