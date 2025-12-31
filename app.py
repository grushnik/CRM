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
# 🎄 CHRISTMAS BACKGROUND (SAFE FOR STREAMLIT CLOUD)
# -------------------------------------------------------------
def inject_christmas_background():
    import base64

    svg = """
    <svg xmlns="http://www.w3.org/2000/svg" width="260" height="260">
      <rect width="260" height="260" fill="none"/>
      <g stroke="rgba(140,80,255,0.35)" stroke-width="2">
        <path d="M40 50 l10 10 M50 50 l-10 10 M45 42 v16 M37 50 h16"/>
        <path d="M200 70 l10 10 M210 70 l-10 10 M205 62 v16 M197 70 h16"/>
        <path d="M120 190 l10 10 M130 190 l-10 10 M125 182 v16 M117 190 h16"/>
        <path d="M70 160 l8 8 M78 160 l-8 8 M74 154 v12 M68 160 h12"/>
        <path d="M190 170 l8 8 M198 170 l-8 8 M194 164 v12 M188 170 h12"/>
      </g>
      <g fill="rgba(140,80,255,0.18)">
        <circle cx="95" cy="35" r="2"/>
        <circle cx="160" cy="120" r="2"/>
        <circle cx="30" cy="210" r="2"/>
        <circle cx="235" cy="220" r="2"/>
        <circle cx="220" cy="25" r="2"/>
      </g>
    </svg>
    """.strip()

    b64 = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
    bg_url = f"data:image/svg+xml;base64,{b64}"

    st.markdown(
        f"""
        <style>
        [data-testid="stAppViewContainer"] {{
            background-image: url("{bg_url}");
            background-repeat: repeat;
            background-size: 260px 260px;
            background-attachment: fixed;
            background-color: #ffffff;
        }}
        .stApp,
        [data-testid="stHeader"],
        [data-testid="stToolbar"],
        [data-testid="stSidebar"] > div:first-child {{
            background: transparent !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


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
    code_in = st.sidebar.text_input("Enter 6-digit code", max_chars=6, key="otp_in")

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
        if not ss.get("otp_delivery_ok", False):
            st.write(ss.get("otp_delivery_msg") or "Telegram delivery failed.")
            st.warning(f"Fallback one-time code (use only if needed): **{ss.get('otp_code','')}**")

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
    # ✅ FIX: make filters consistent by using lower() comparisons
    sql = """
        SELECT *,
               (SELECT MAX(ts) FROM notes n WHERE n.contact_id = c.id) AS last_note_ts
        FROM contacts c
        WHERE 1=1
    """
    params: List[Any] = []

    if q:
        like = f"%{q.strip().lower()}%"
        sql += """
            AND (
                lower(COALESCE(first_name,'')) LIKE ?
                OR lower(COALESCE(last_name,'')) LIKE ?
                OR lower(COALESCE(email,'')) LIKE ?
                OR lower(COALESCE(company,'')) LIKE ?
            )
        """
        params += [like, like, like, like]

    if cats:
        sql += " AND category IN (" + ",".join("?" for _ in cats) + ")"
        params += cats

    if stats:
        sql += " AND status IN (" + ",".join("?" for _ in stats) + ")"
        params += stats

    if state_like:
        sql += " AND lower(COALESCE(state,'')) LIKE ?"
        params.append(f"%{state_like.strip().lower()}%")

    if app_filter:
        sql += " AND application IN (" + ",".join("?" for _ in app_filter) + ")"
        params += app_filter

    if prod_filter:
        sql += " AND product_interest IN (" + ",".join("?" for _ in prod_filter) + ")"
        params += prod_filter

    sql += " ORDER BY COALESCE(last_name,''), COALESCE(first_name,''), id DESC"
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
# SALES HELPERS
# -------------------------------------------------------------
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


def add_sale_line(
    conn: sqlite3.Connection,
    contact_id: int,
    sold_at: date,
    product: str,
    qty: int,
    unit_price_usd: float,
    note: str = "",
):
    qty = int(qty) if qty is not None else 1
    qty = max(qty, 1)
    cents = _usd_to_cents(unit_price_usd)
    if cents is None:
        raise ValueError("Invalid price")
    sold_at_iso = sold_at.isoformat() if isinstance(sold_at, date) else datetime.utcnow().date().isoformat()
    conn.execute(
        """
        INSERT INTO sales(contact_id, sold_at, product, qty, unit_price_cents, currency, note)
        VALUES (?,?,?,?,?,?,?)
        """,
        (int(contact_id), sold_at_iso, (product or "").strip(), qty, int(cents), "USD", (note or "").strip() or None),
    )
    conn.commit()


def delete_sale_line(conn: sqlite3.Connection, sale_id: int):
    conn.execute("DELETE FROM sales WHERE id=?", (int(sale_id),))
    conn.commit()


def get_sales_for_contact(conn: sqlite3.Connection, contact_id: int) -> pd.DataFrame:
    try:
        cols = _table_cols(conn, "sales")
    except Exception:
        return pd.DataFrame(columns=["id", "sold_at", "product", "qty", "unit_price_cents", "currency", "note"])

    wanted = ["id", "sold_at", "product", "qty", "unit_price_cents", "currency", "note"]
    select_cols = [c for c in wanted if c in cols]

    if not select_cols:
        return pd.DataFrame(columns=wanted)

    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM sales
        WHERE contact_id=?
        ORDER BY sold_at DESC, id DESC
    """
    df = pd.read_sql_query(sql, conn, params=(int(contact_id),))

    for c in wanted:
        if c not in df.columns:
            df[c] = "" if c in ("currency", "note", "product", "sold_at") else 0
    return df[wanted]


def get_sales_agg(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT
          s.contact_id,
          COALESCE(SUM(s.qty),0) AS sold_qty,
          COALESCE(SUM(s.qty * s.unit_price_cents),0) AS sold_revenue_cents,
          MIN(s.sold_at) AS first_sold_at,
          MAX(s.sold_at) AS last_sold_at
        FROM sales s
        GROUP BY s.contact_id
        """,
        conn,
    )
    if df.empty:
        return pd.DataFrame(
            {
                "contact_id": pd.Series(dtype="int64"),
                "sold_qty": pd.Series(dtype="int64"),
                "sold_revenue_cents": pd.Series(dtype="int64"),
                "first_sold_at": pd.Series(dtype="object"),
                "last_sold_at": pd.Series(dtype="object"),
                "sales_lines": pd.Series(dtype="object"),
                "sold_revenue_usd": pd.Series(dtype="float64"),
            }
        )

    df["contact_id"] = safe_int_series(df["contact_id"], 0)
    df["sold_qty"] = safe_int_series(df["sold_qty"], 0)
    df["sold_revenue_cents"] = safe_int_series(df["sold_revenue_cents"], 0)

    df_lines = pd.read_sql_query(
        "SELECT contact_id, sold_at, product, qty, unit_price_cents FROM sales ORDER BY sold_at ASC, id ASC",
        conn,
    )

    def fmt_line(r):
        d = (str(r["sold_at"]) or "")[:10]
        price = int(pd.to_numeric(r["unit_price_cents"], errors="coerce") or 0) / 100.0
        q = int(pd.to_numeric(r["qty"], errors="coerce") or 0)
        return f"{d}: {r['product']} x{q} @ ${price:,.0f}"

    if not df_lines.empty:
        df_lines["contact_id"] = safe_int_series(df_lines["contact_id"], 0)
        df_lines["qty"] = safe_int_series(df_lines["qty"], 0)
        df_lines["unit_price_cents"] = safe_int_series(df_lines["unit_price_cents"], 0)

        lines = (
            df_lines.assign(_l=df_lines.apply(fmt_line, axis=1))
            .groupby("contact_id")["_l"]
            .apply(lambda s: " | ".join(s.tolist()))
            .reset_index(name="sales_lines")
        )
        df = df.merge(lines, on="contact_id", how="left")
    else:
        df["sales_lines"] = ""

    df["sold_revenue_usd"] = safe_float_series(df["sold_revenue_cents"], 0.0) / 100.0
    return df


def get_sales_yearly_totals(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT
          CAST(strftime('%Y', sold_at) AS INTEGER) AS year,
          COALESCE(SUM(qty),0) AS qty,
          COALESCE(SUM(qty * unit_price_cents),0) AS revenue_cents
        FROM sales
        GROUP BY CAST(strftime('%Y', sold_at) AS INTEGER)
        ORDER BY year ASC
        """,
        conn,
    )
    if df.empty:
        return pd.DataFrame(columns=["year", "qty", "revenue_usd"])
    df["qty"] = safe_int_series(df["qty"], 0)
    df["revenue_cents"] = safe_int_series(df["revenue_cents"], 0)
    df["revenue_usd"] = df["revenue_cents"] / 100.0
    return df[["year", "qty", "revenue_usd"]]


# -------------------------------------------------------------
# CRM STATS: conversion + speed
# -------------------------------------------------------------
def _try_parse_iso(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        return dtparser.parse(s)
    except Exception:
        return None


def get_conversion_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    df_hist = pd.read_sql_query(
        "SELECT contact_id, ts, new_status FROM status_history ORDER BY contact_id, ts",
        conn,
    )
    df_contacts = pd.read_sql_query("SELECT id, status FROM contacts", conn)

    first_contacted: Dict[int, datetime] = {}
    first_won_status: Dict[int, datetime] = {}

    for r in df_hist.itertuples(index=False):
        cid = int(r.contact_id)
        ts = _try_parse_iso(r.ts)
        if not ts:
            continue
        ns = (r.new_status or "").strip()
        if ns == "Contacted" and cid not in first_contacted:
            first_contacted[cid] = ts
        if ns == "Won" and cid not in first_won_status:
            first_won_status[cid] = ts

    df_sales_min = pd.read_sql_query(
        "SELECT contact_id, MIN(sold_at) AS first_sold_at FROM sales GROUP BY contact_id",
        conn,
    )
    first_sold: Dict[int, datetime] = {}
    for r in df_sales_min.itertuples(index=False):
        cid = int(r.contact_id)
        ts = _try_parse_iso(r.first_sold_at)
        if ts:
            first_sold[cid] = ts

    contacted_like = {
        "Contacted",
        "Meeting",
        "Quoted",
        "Won",
        "Lost",
        "Nurture",
        "Pending",
        "On hold",
        "Irrelevant",
    }
    contacted_set = set(first_contacted.keys())
    for r in df_contacts.itertuples(index=False):
        cid = int(r.id)
        stt = (r.status or "New").strip()
        if stt in contacted_like:
            contacted_set.add(cid)

    won_set = set(first_sold.keys())
    won_set |= set(int(r.id) for r in df_contacts.itertuples(index=False) if (r.status or "").strip() == "Won")

    contacted_count = len(contacted_set)
    won_count = len(won_set)
    conversion_rate = (won_count / contacted_count) if contacted_count else 0.0

    deltas = []
    for cid, t_contacted in first_contacted.items():
        t_win = first_sold.get(cid) or first_won_status.get(cid)
        if t_win and t_win >= t_contacted:
            deltas.append((t_win - t_contacted).total_seconds() / 86400.0)
    avg_days = sum(deltas) / len(deltas) if deltas else None

    return {
        "contacted_count": contacted_count,
        "won_count": won_count,
        "conversion_rate": conversion_rate,
        "avg_days_contacted_to_win": avg_days,
        "speed_n": len(deltas),
    }


# -------------------------------------------------------------
# TOP COUNTERS (torches + revenue)
# -------------------------------------------------------------
def show_sales_counters(conn: sqlite3.Connection):
    df_qty = pd.read_sql_query("SELECT COALESCE(SUM(qty),0) AS q FROM sales", conn)
    total_qty = int(pd.to_numeric(df_qty.iloc[0]["q"], errors="coerce") or 0) if not df_qty.empty else 0

    yearly = get_sales_yearly_totals(conn)
    year_map = {int(r.year): float(r.revenue_usd) for r in yearly.itertuples(index=False)} if not yearly.empty else {}

    current_year = datetime.utcnow().year
    start_year = 2025
    years = list(range(start_year, current_year + 1))

    df_companies = pd.read_sql_query(
        """
        SELECT DISTINCT TRIM(c.company) AS company
        FROM sales s
        JOIN contacts c ON c.id = s.contact_id
        WHERE c.company IS NOT NULL AND TRIM(c.company) <> ''
        ORDER BY company
        """,
        conn,
    )
    companies = df_companies["company"].dropna().tolist() if not df_companies.empty else []

    lines = []
    for y in years[-3:]:
        rev = year_map.get(y, 0.0)
        lines.append(f"<div style='font-size:12px;opacity:0.88;'>Revenue {y}: <b>${rev:,.0f}</b></div>")

    st.markdown(
        f"""
        <div style="
            text-align:right;
            padding:10px 14px;
            border-radius:14px;
            background: linear-gradient(135deg, #8b2cff 0%, #5a22ff 45%, #a100ff 100%);
            color:#fff;
            font-family:system-ui, sans-serif;
            box-shadow: 0 8px 18px rgba(130, 46, 255, 0.25);
            min-width: 260px;
        ">
            <div style="font-size:12px; opacity:0.85; letter-spacing:0.2px;">Torches sold (all time)</div>
            <div style="font-size:34px; font-weight:800; line-height:1;">{total_qty}</div>
            <div style="margin-top:6px;">
                {''.join(lines) if lines else "<div style='font-size:12px;opacity:0.88;'>Revenue: $0</div>"}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Sold to: " + " • ".join(companies) if companies else "Sold to: no customers yet")


def show_dashboard_strip(conn: sqlite3.Connection):
    c1, c2, c3, c4 = st.columns([1.4, 1, 1, 1.2])
    with c1:
        show_sales_counters(conn)

    stats = get_conversion_stats(conn)
    with c2:
        st.metric("Contacted leads", stats["contacted_count"])
    with c3:
        st.metric("Won leads", stats["won_count"])
    with c4:
        st.metric("Contacted → Won", f"{stats['conversion_rate']*100:.1f}%")

    if stats["avg_days_contacted_to_win"] is not None:
        st.caption(
            f"Avg speed (first Contacted → first Win/Sale): **{stats['avg_days_contacted_to_win']:.1f} days** (n={stats['speed_n']})"
        )
    else:
        st.caption("Speed metric: not enough status-history data yet (needs Contacted timestamps).")


# -------------------------------------------------------------
# OVERVIEW LISTS (HTML)
# -------------------------------------------------------------
def _render_lead_list(title_html: str, df: pd.DataFrame):
    st.markdown(title_html, unsafe_allow_html=True)
    if df.empty:
        st.caption("No leads in this group.")
        return

    rows_html = []
    for _, sub in df.iterrows():
        first = (sub.get("first_name") or "").strip()
        last = (sub.get("last_name") or "").strip()
        lead = f"{first} {last}".strip() or "—"

        flag = flag_img(sub.get("country"))
        profile = _clean_url(sub.get("profile_url") or "")
        company = (sub.get("company") or "").strip()
        email = (sub.get("email") or "").strip()
        status = (sub.get("status") or "").strip()

        product = (sub.get("product_interest") or "").strip()
        application = (sub.get("application") or "").strip()

        meta_bits = []
        if company:
            meta_bits.append(company)
        if email:
            meta_bits.append(email)
        if product:
            meta_bits.append(f"interested in {product}")
        if application:
            meta_bits.append(f"application: {application}")
        meta = " • ".join(meta_bits) if meta_bits else "—"

        if profile:
            profile_icon_html = f"<a href='{profile}' target='_blank' style='text-decoration:none;font-size:16px;'>👤</a>"
        else:
            profile_icon_html = "<span style='font-size:16px;'>👤</span>"

        status_badge = (
            f"<span style='padding:2px 8px;border-radius:999px;background:rgba(255,255,255,0.10);font-size:12px;'>{status}</span>"
            if status
            else ""
        )

        rows_html.append(
            f"""
        <div style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid rgba(0,0,0,0.06);">
          <div style="flex:0 0 auto;margin-top:2px;">{profile_icon_html}</div>
          <div style="flex:1 1 auto;min-width:0;">
            <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;">
              <div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                {lead} {flag}
              </div>
              <div style="flex:0 0 auto;">{status_badge}</div>
            </div>
            <div style="font-size:12px;opacity:0.75;margin-top:2px;line-height:1.2;">{meta}</div>
          </div>
        </div>
        """
        )

    block = f"<div style='font-family:system-ui, -apple-system, Segoe UI, Roboto, Arial;'>{''.join(rows_html)}</div>"
    est_height = min(1200, 54 * len(df) + 60)
    components.html(block, height=est_height, scrolling=True)


def show_priority_lists(conn: sqlite3.Connection):
    st.subheader("Customer overview")

    show_dashboard_strip(conn)
    st.markdown("---")

    df_all = pd.read_sql_query(
        "SELECT id, first_name, last_name, company, email, status, owner, profile_url, country, product_interest, application FROM contacts",
        conn,
    )
    if df_all.empty:
        st.caption("No contacts yet – add someone manually or import a file.")
        return

    df_all["status"] = df_all["status"].fillna("New").astype(str).str.strip()

    st.caption("⚡ Quick move lead between buckets")
    options = {
        int(r.id): f"{(r.first_name or '')} {(r.last_name or '')} — {r.company or ''} ({r.email or ''})"
        for r in df_all.itertuples(index=False)
    }

    q1, q2, q3 = st.columns([2.6, 1.2, 1.2])
    with q1:
        picked = st.selectbox("Pick lead", list(options.keys()), format_func=lambda cid: options.get(cid, str(cid)), key="ov_pick_lead")
    with q2:
        new_status = st.selectbox("New status", PIPELINE, index=PIPELINE.index("New") if "New" in PIPELINE else 0, key="ov_new_status")
    with q3:
        st.write("")
        st.write("")
        if st.button("Move / Update status", use_container_width=True, key="ov_move_status"):
            update_contact_status(conn, int(picked), str(new_status))
            st.success("Updated.")
            st.rerun()

    st.markdown("---")

    hot_raw = df_all[df_all["status"].isin(["Quoted", "Meeting"])].copy()
    pot_raw = df_all[df_all["status"].isin(["New", "Contacted"])].copy()
    cold_raw = df_all[df_all["status"].isin(["Pending", "On hold", "Irrelevant"])].copy()

    col1, col2, col3 = st.columns(3)

    with col1:
        hot_header = f"""
            <div style="background-color:#ff6b6b;padding:6px 10px;border-radius:10px;
                        font-weight:700;color:white;text-align:center;margin-bottom:6px;">
                🔥 Hot customers ({len(hot_raw)}) — Quoted / Meeting
            </div>
        """
        _render_lead_list(hot_header, hot_raw)

    with col2:
        pot_header = f"""
            <div style="background-color:#28a745;padding:6px 10px;border-radius:10px;
                        font-weight:700;color:white;text-align:center;margin-bottom:6px;">
                🌱 Potential customers ({len(pot_raw)}) — New / Contacted
            </div>
        """
        _render_lead_list(pot_header, pot_raw)

    with col3:
        cold_header = f"""
            <div style="background-color:#007bff;padding:6px 10px;border-radius:10px;
                        font-weight:700;color:white;text-align:center;margin-bottom:6px;">
                ❄️ Cold customers ({len(cold_raw)}) — Pending / On hold / Irrelevant
            </div>
        """
        _render_lead_list(cold_header, cold_raw)


# -------------------------------------------------------------
# SIDEBAR IMPORT / EXPORT + DEDUPE BUTTON
# -------------------------------------------------------------
def sidebar_import_export(conn: sqlite3.Connection):
    st.sidebar.header("Import / Export")

    if st.sidebar.button("🧹 Deduplicate database now"):
        removed = dedupe_database(conn)
        st.sidebar.success(f"Removed {removed} duplicate contacts")
        st.rerun()

    up = st.sidebar.file_uploader("Upload Excel/CSV (Contacts)", type=["xlsx", "xls", "csv"])
    if up is not None:
        df = load_contacts_file(up)
        n = upsert_contacts(conn, df)
        st.sidebar.success(f"Imported/updated {n} contacts")
        st.rerun()

    total = pd.read_sql_query("SELECT COUNT(*) n FROM contacts", conn).iloc[0]["n"]
    st.sidebar.caption(f"Total contacts: **{int(total)}**")

    export_df = st.session_state.get("export_df")
    if isinstance(export_df, pd.DataFrame) and not export_df.empty:
        csv_bytes = export_df.to_csv(index=False, quoting=csv.QUOTE_ALL).encode("utf-8")
        st.sidebar.download_button("Download Contacts CSV (filtered)", csv_bytes, file_name="radom-contacts.csv")


# -------------------------------------------------------------
# FILTERS UI  ✅ FIXED (keys!)
# -------------------------------------------------------------
def filters_ui():
    st.subheader("Filters")

    q = st.text_input("Search (name, email, company)", "", key="flt_q")

    c1, c2, c3 = st.columns(3)
    with c1:
        cats = st.multiselect(
            "Category",
            ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"],
            [],
            key="flt_cats",
        )
    with c2:
        stats = st.multiselect("Status", PIPELINE, [], key="flt_stats")
    with c3:
        st_like = st.text_input("State/Province contains", "", key="flt_state_like")

    c4, c5 = st.columns(2)
    with c4:
        app_filter = st.multiselect("Application", APPLICATIONS, [], key="flt_apps")
    with c5:
        prod_filter = st.multiselect("Product type interest", PRODUCTS, [], key="flt_prods")

    return q, cats, stats, st_like, app_filter, prod_filter


# -------------------------------------------------------------
# EXPORT BUILD (includes notes + sales)
# -------------------------------------------------------------
def build_export_df(conn: sqlite3.Connection, base_df: pd.DataFrame) -> pd.DataFrame:
    if base_df.empty:
        return base_df

    notes = get_notes_agg(conn)
    sales = get_sales_agg(conn)

    out = base_df.copy()
    out["id"] = safe_int_series(out["id"], 0)

    if not notes.empty:
        notes["contact_id"] = safe_int_series(notes["contact_id"], 0)
        out = out.merge(notes, left_on="id", right_on="contact_id", how="left").drop(columns=["contact_id"])
    else:
        out["notes"] = ""

    if not sales.empty:
        sales["contact_id"] = safe_int_series(sales["contact_id"], 0)
        out = out.merge(sales, left_on="id", right_on="contact_id", how="left").drop(columns=["contact_id"])
    else:
        out["sold_qty"] = 0
        out["sold_revenue_cents"] = 0
        out["sold_revenue_usd"] = 0.0
        out["first_sold_at"] = ""
        out["last_sold_at"] = ""
        out["sales_lines"] = ""

    out["sold_qty"] = safe_int_series(out.get("sold_qty", pd.Series([0] * len(out))), 0)
    out["sold_revenue_cents"] = safe_int_series(out.get("sold_revenue_cents", pd.Series([0] * len(out))), 0)
    out["sold_revenue_usd"] = safe_float_series(out.get("sold_revenue_usd", pd.Series([0.0] * len(out))), 0.0)

    return out.fillna("")


# -------------------------------------------------------------
# CONTACT EDITOR
# -------------------------------------------------------------
def contact_editor(conn: sqlite3.Connection, row: pd.Series):
    st.markdown("---")
    contact_id = int(row["id"])

    st.markdown(f"### ✏️ {row.get('first_name','')} {row.get('last_name','')} — {row.get('company') or ''}")
    st.caption(
        f"Status: {row.get('status') or 'New'} | "
        f"Application: {row.get('application') or ''} | "
        f"Product: {row.get('product_interest') or ''}"
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        first_name = st.text_input("First name", value=str(row.get("first_name") or ""), key=f"fn_{contact_id}")
        last_name = st.text_input("Last name", value=str(row.get("last_name") or ""), key=f"ln_{contact_id}")
        job_title = st.text_input("Job title", value=str(row.get("job_title") or ""), key=f"jt_{contact_id}")
    with c2:
        company = st.text_input("Company", value=str(row.get("company") or ""), key=f"co_{contact_id}")
        email = st.text_input("Email", value=str(row.get("email") or ""), key=f"em_{contact_id}")
        phone = st.text_input("Phone", value=str(row.get("phone") or ""), key=f"ph_{contact_id}")
    with c3:
        website = st.text_input("Website", value=str(row.get("website") or ""), key=f"wb_{contact_id}")
        profile_url = st.text_input(
            "LinkedIn/Profile URL", value=str(row.get("profile_url") or ""), key=f"li_{contact_id}"
        )
        owner = st.selectbox(
            "Owner",
            OWNERS,
            index=OWNERS.index(row.get("owner") or "") if (row.get("owner") or "") in OWNERS else 0,
            key=f"ow_{contact_id}",
        )

    c4, c5, c6 = st.columns(3)
    with c4:
        status = st.selectbox(
            "Status",
            PIPELINE,
            index=PIPELINE.index((row.get("status") or "New"))
            if (row.get("status") or "New") in PIPELINE
            else 0,
            key=f"st_{contact_id}",
        )
        gender = st.text_input("Gender", value=str(row.get("gender") or ""), key=f"ge_{contact_id}")
    with c5:
        application = st.selectbox(
            "Application",
            [""] + APPLICATIONS,
            index=([""] + APPLICATIONS).index(row.get("application") or "")
            if (row.get("application") or "") in ([""] + APPLICATIONS)
            else 0,
            key=f"ap_{contact_id}",
        )
        product_interest = st.selectbox(
            "Product interest",
            [""] + PRODUCTS,
            index=([""] + PRODUCTS).index(row.get("product_interest") or "")
            if (row.get("product_interest") or "") in ([""] + PRODUCTS)
            else 0,
            key=f"pi_{contact_id}",
        )
    with c6:
        country = st.text_input("Country", value=str(row.get("country") or ""), key=f"ct_{contact_id}")
        state = st.text_input("State/Province", value=str(row.get("state") or ""), key=f"stt_{contact_id}")
        city = st.text_input("City", value=str(row.get("city") or ""), key=f"ci_{contact_id}")

    addr1 = st.text_input("Street", value=str(row.get("street") or ""), key=f"a1_{contact_id}")
    addr2 = st.text_input("Street 2", value=str(row.get("street2") or ""), key=f"a2_{contact_id}")
    zip_code = st.text_input("Zip", value=str(row.get("zip_code") or ""), key=f"zp_{contact_id}")

    dedupe_key = compute_dedupe_key(first_name, last_name, company, email, profile_url)

    csave1, csave2, _ = st.columns([1, 1, 3])
    with csave1:
        if st.button("💾 Save contact", key=f"save_{contact_id}"):
            current_status = (row.get("status") or "New").strip()
            new_status = (status or "New").strip()
            if current_status != new_status:
                update_contact_status(conn, contact_id, new_status)

            conn.execute(
                """
                UPDATE contacts SET
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
                  owner=?,
                  gender=?,
                  application=?,
                  product_interest=?,
                  profile_url=?,
                  dedupe_key=?
                WHERE id=?
                """,
                (
                    first_name.strip() or None,
                    last_name.strip() or None,
                    job_title.strip() or None,
                    company.strip() or None,
                    addr1.strip() or None,
                    addr2.strip() or None,
                    zip_code.strip() or None,
                    city.strip() or None,
                    state.strip() or None,
                    country.strip() or None,
                    phone.strip() or None,
                    _norm_email(email) or None,
                    _clean_url(website) or None,
                    owner.strip() or None,
                    gender.strip() or None,
                    normalize_application(application) if application else None,
                    product_interest.strip() or None,
                    _clean_url(profile_url) or None,
                    dedupe_key or None,
                    contact_id,
                ),
            )
            conn.commit()
            backup_contacts(conn)
            ensure_dedupe_index(conn)
            st.success("Saved.")
            st.rerun()

    with csave2:
        if st.button("🗑️ Delete contact", key=f"del_{contact_id}"):
            conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
            conn.commit()
            backup_contacts(conn)
            st.warning("Deleted.")
            st.rerun()

    # Notes
    st.markdown("#### 📝 Notes")
    notes_df = get_notes(conn, contact_id)
    if not notes_df.empty:
        for r in notes_df.itertuples(index=False):
            ts = (str(r.ts) or "")[:19]
            body = str(r.body or "")
            nf = str(r.next_followup or "")
            st.markdown(f"- **{ts}** — {body}" + (f" _(follow-up: {nf})_" if nf else ""))

    new_note = st.text_area("Add note", key=f"new_note_{contact_id}")
    next_followup = st.text_input("Next follow-up (optional)", key=f"nf_{contact_id}", value="")
    if st.button("➕ Add note", key=f"add_note_{contact_id}"):
        body = sanitize_note_text(new_note, trim_email_threads=False)
        if body:
            ts_iso = datetime.utcnow().isoformat()
            conn.execute(
                "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                (contact_id, ts_iso, body, next_followup.strip() or None),
            )
            conn.commit()
            backup_contacts(conn)
            st.success("Note added.")
            st.rerun()
        else:
            st.info("Empty note ignored.")

    # Sales
    st.markdown("#### 💰 Sales")
    sales_df = get_sales_for_contact(conn, contact_id)
    if not sales_df.empty:
        sales_show = sales_df.copy()
        sales_show["qty"] = safe_int_series(sales_show["qty"], 0)
        sales_show["unit_price_cents"] = safe_int_series(sales_show["unit_price_cents"], 0)
        sales_show["unit_price_usd"] = sales_show["unit_price_cents"] / 100.0
        st.dataframe(sales_show.drop(columns=["unit_price_cents"]), use_container_width=True)
    else:
        st.caption("No sales for this contact yet.")

    sc1, sc2, sc3, sc4 = st.columns([1.2, 1, 1, 2])
    with sc1:
        sold_at = st.date_input("Sold at", value=datetime.utcnow().date(), key=f"sold_at_{contact_id}")
    with sc2:
        product = st.selectbox("Product", PRODUCTS, index=0, key=f"prod_{contact_id}")
    with sc3:
        qty = st.number_input("Qty", min_value=1, max_value=1000, value=1, step=1, key=f"qty_{contact_id}")
    with sc4:
        unit_price = st.number_input(
            "Unit price (USD)", min_value=0.0, value=0.0, step=1000.0, key=f"price_{contact_id}"
        )

    sale_note = st.text_input("Sale note (optional)", key=f"sale_note_{contact_id}", value="")
    if st.button("➕ Add sale line", key=f"add_sale_{contact_id}"):
        try:
            add_sale_line(conn, contact_id, sold_at, product, int(qty), float(unit_price), sale_note)
            st.success("Sale added.")
            st.rerun()
        except Exception as e:
            st.error(f"Could not add sale: {e}")

    if not sales_df.empty:
        sale_ids = sales_df["id"].tolist()
        del_id = st.selectbox("Delete sale line id", sale_ids, key=f"del_sale_pick_{contact_id}")
        if st.button("Delete selected sale line", key=f"del_sale_btn_{contact_id}"):
            delete_sale_line(conn, int(del_id))
            st.warning("Sale line deleted.")
            st.rerun()


# -------------------------------------------------------------
# DASHBOARD
# -------------------------------------------------------------
def revenue_histogram(conn: sqlite3.Connection):
    st.subheader("Total revenue by year")

    yearly = get_sales_yearly_totals(conn)
    actual = {int(r.year): float(r.revenue_usd) for r in yearly.itertuples(index=False)} if not yearly.empty else {}

    projections = {2026: 200000.0}

    years = sorted(set(actual.keys()) | set(projections.keys()) | {2025, 2026})

    rows = []
    for y in years:
        is_projected = False
        if y in actual and actual[y] > 0:
            val = actual[y]
        else:
            val = float(projections.get(y, 0.0))
            is_projected = (y in projections)

        rows.append({"Year": str(y) + (" (proj)" if is_projected else ""), "Revenue": float(val), "_year_num": int(y)})

    chart_df = pd.DataFrame(rows).sort_values("_year_num")[["Year", "Revenue"]].reset_index(drop=True)
    st.bar_chart(chart_df, x="Year", y="Revenue")

    if 2025 in actual:
        st.caption(f"2025 actual revenue from DB: **${actual[2025]:,.0f}**")
    else:
        st.caption("2025 has no sales in DB yet (chart shows 0 unless you add sales lines).")


def dashboard(conn: sqlite3.Connection):
    st.subheader("Dashboard")
    show_dashboard_strip(conn)
    st.markdown("---")
    revenue_histogram(conn)


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    inject_christmas_background()

    conn = get_conn()
    init_db(conn)
    restore_from_backup_if_empty(conn)
    ensure_dedupe_index(conn)

    check_login_two_factor_telegram()

    st.title(APP_TITLE)
    sidebar_import_export(conn)

    # ✅ Tab order: Overview first, Contacts second, Dashboard third
    tab_overview, tab_contacts, tab_dashboard = st.tabs(["🔥 Overview", "📋 Contacts", "📊 Dashboard"])

    with tab_overview:
        show_priority_lists(conn)

    with tab_contacts:
        q, cats, stats, st_like, app_filter, prod_filter = filters_ui()
        df = query_contacts(conn, q, cats, stats, st_like, app_filter, prod_filter)

        st.caption(f"Filtered results: **{len(df)}**")

        export_df = build_export_df(conn, df)
        st.session_state["export_df"] = export_df

        if df.empty:
            st.info("No contacts match filters.")
            return

        view = df.copy()
        view["id"] = safe_int_series(view["id"], 0)
        for c in ["first_name", "last_name", "company", "email", "status", "owner", "application", "product_interest", "last_note_ts"]:
            if c not in view.columns:
                view[c] = ""
        view = view[
            ["id", "first_name", "last_name", "company", "email", "status", "owner", "application", "product_interest", "last_note_ts"]
        ].fillna("")

        st.dataframe(view, use_container_width=True, hide_index=True)

        # ✅ FIX: robust picker that resets when filters change
        df2 = df.copy()
        df2["id"] = safe_int_series(df2["id"], 0)

        options = []
        labels = []
        for r in df2.itertuples(index=False):
            cid = int(r.id)
            name = f"{(r.first_name or '')} {(r.last_name or '')}".strip() or f"ID {cid}"
            company = (r.company or "").strip()
            email = (r.email or "").strip()
            extra = " — ".join([x for x in [company, email] if x])
            label = f"{name} — {extra}" if extra else name
            options.append(cid)
            labels.append(label)

        if not options:
            st.info("No contacts match filters.")
            return

        ss = st.session_state
        if ss.get("picked_contact_id") not in options:
            ss["picked_contact_id"] = options[0]

        picked = st.selectbox(
            "Select contact to edit",
            options,
            index=options.index(ss["picked_contact_id"]),
            format_func=lambda cid: labels[options.index(cid)],
            key="picked_contact_id",
        )

        row = df2[df2["id"] == int(picked)].iloc[0]
        contact_editor(conn, row)

    with tab_dashboard:
        dashboard(conn)


if __name__ == "__main__":
    main()
