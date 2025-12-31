# app.py — Radom CRM (Streamlit)
# Adds:
# 1) Sales ledger (multiple torches per won lead, with qty + unit price)
# 2) Top counters: total torches sold + revenue current year + revenue-by-year line
# 3) Export includes sales aggregates + readable sales lines
# 4) Pipeline stats: Contacted→Won conversion + speed (days-to-win)

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
DB_FILE = "data/radom_crm.db"
BACKUP_FILE = "data/contacts_backup.csv"

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
# ✅ FIX NOTES IMPORT (sanitize + optionally trim email threads)
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
    """
    Converts multi-line CRM-exported notes into a single-line safe note.
    Optionally trims quoted email threads (everything after 'On ... wrote:' etc.).
    """
    if v is None:
        return ""
    s = str(v)

    # normalize newlines
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    if trim_email_threads:
        for marker in _EMAIL_THREAD_MARKERS:
            if marker in s:
                s = s.split(marker)[0]
                break
        s = re.split(r"\nOn\s.+\swrote:\s*\n", s, maxsplit=1)[0]

    # collapse whitespace/newlines into one line
    s = re.sub(r"\n+", " ⏎ ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if max_len and len(s) > max_len:
        s = s[:max_len].rstrip() + "…"

    return s


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
# DEDUPE KEY (core of "no duplicates")
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
    s = re.sub(r"[?#].*$", "", s)  # strip query/hash
    s = s.rstrip("/")
    return s


def compute_dedupe_key(first: Any, last: Any, company: Any, email: Any, profile_url: Any) -> str:
    """
    Priority:
    1) email
    2) profile_url
    3) name+company
    """
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
# TELEGRAM OTP (MULTI-USER)
# -------------------------------------------------------------
def _tg_token() -> str:
    return str(st.secrets.get("TELEGRAM_BOT_TOKEN", "")).strip()


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
    expected = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)

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

        admin_chat_id = st.secrets.get("ADMIN_CHAT_ID", "")
        if admin_chat_id:
            if st.button("Send test message to admin_chat_id"):
                ok, msg = telegram_send_message(
                    int(admin_chat_id),
                    "✅ Telegram test from Radom CRM (admin_chat_id)",
                )
                st.write("Test message sent." if ok else "Failed to send.")
                st.code(msg)
        else:
            st.caption("Tip: set ADMIN_CHAT_ID in secrets to enable test-send button.")

    st.stop()


# -------------------------------------------------------------
# DB + BACKUP HELPERS
# -------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


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
          email TEXT UNIQUE,
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

        -- ✅ Sales ledger: one row per line item
        CREATE TABLE IF NOT EXISTS sales (
          id INTEGER PRIMARY KEY,
          contact_id INTEGER NOT NULL,
          sold_at TEXT NOT NULL,             -- ISO timestamp/date
          year INTEGER NOT NULL,             -- extracted from sold_at
          product TEXT NOT NULL,             -- e.g. "1 kW", "10 kW", ...
          qty INTEGER NOT NULL DEFAULT 1,
          unit_price_cents INTEGER NOT NULL DEFAULT 0,   -- store money safely
          currency TEXT NOT NULL DEFAULT 'USD',
          FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        );
        """
    )

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(contacts)")
    cols = [row[1] for row in cur.fetchall()]

    add_cols = {
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
    }
    for c, t in add_cols.items():
        if c not in cols:
            cur.execute(f"ALTER TABLE contacts ADD COLUMN {c} {t}")

    conn.commit()


def backup_contacts(conn: sqlite3.Connection):
    df = pd.read_sql_query("SELECT * FROM contacts", conn)
    if not df.empty:
        os.makedirs("data", exist_ok=True)
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
# SALES HELPERS (ledger + metrics + aggregation)
# -------------------------------------------------------------
def add_sales_lines(
    conn: sqlite3.Connection,
    contact_id: int,
    sold_date: date,
    lines: List[Dict[str, Any]],
    currency: str = "USD",
):
    """
    lines: [{"product":"10 kW","qty":2,"unit_price":125000.0}, ...]
    unit_price is in dollars; stored as cents.
    """
    cur = conn.cursor()
    sold_at_iso = datetime.combine(sold_date, datetime.min.time()).isoformat()
    yr = sold_date.year

    for ln in lines:
        product = (ln.get("product") or "").strip()
        qty = int(ln.get("qty") or 0)
        unit_price = float(ln.get("unit_price") or 0.0)
        if not product or qty <= 0:
            continue
        unit_cents = int(round(unit_price * 100.0))

        cur.execute(
            """
            INSERT INTO sales(contact_id, sold_at, year, product, qty, unit_price_cents, currency)
            VALUES (?,?,?,?,?,?,?)
            """,
            (contact_id, sold_at_iso, yr, product, qty, unit_cents, currency),
        )

    conn.commit()
    backup_contacts(conn)


def get_sales_agg(conn: sqlite3.Connection) -> pd.DataFrame:
    # One row per contact_id: totals + first/last sale date
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

    # Readable line items string per contact
    df_lines = pd.read_sql_query(
        """
        SELECT contact_id, sold_at, product, qty, unit_price_cents
        FROM sales
        ORDER BY sold_at ASC, id ASC
        """,
        conn,
    )

    if df.empty:
        return pd.DataFrame(columns=[
            "contact_id", "sold_qty", "sold_revenue_cents", "first_sold_at", "last_sold_at", "sales_lines", "sold_revenue_usd"
        ])

    if df_lines.empty:
        df["sales_lines"] = ""
        df["sold_revenue_usd"] = df["sold_revenue_cents"] / 100.0
        return df

    def fmt_line(r):
        d = (str(r["sold_at"]) or "")[:10]
        price = (int(r["unit_price_cents"]) / 100.0)
        return f"{d}: {r['product']} x{int(r['qty'])} @ ${price:,.0f}"

    lines = (
        df_lines.assign(_l=df_lines.apply(fmt_line, axis=1))
        .groupby("contact_id")["_l"]
        .apply(lambda s: " | ".join(s.tolist()))
        .reset_index(name="sales_lines")
    )

    df = df.merge(lines, on="contact_id", how="left")
    df["sales_lines"] = df["sales_lines"].fillna("")
    df["sold_revenue_usd"] = df["sold_revenue_cents"] / 100.0
    return df


def get_sales_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    df_qty = pd.read_sql_query("SELECT COALESCE(SUM(qty),0) AS n FROM sales", conn)
    total_qty = int(df_qty.iloc[0]["n"]) if not df_qty.empty else 0

    df_rev = pd.read_sql_query(
        """
        SELECT year, COALESCE(SUM(qty * unit_price_cents),0) AS cents
        FROM sales
        GROUP BY year
        ORDER BY year
        """,
        conn,
    )
    rev_by_year = {int(r["year"]): int(r["cents"]) / 100.0 for _, r in df_rev.iterrows()} if not df_rev.empty else {}

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

    return {"total_qty": total_qty, "rev_by_year": rev_by_year, "companies": companies}


def show_sales_counters(conn: sqlite3.Connection):
    m = get_sales_metrics(conn)
    total_qty = m["total_qty"]
    rev_by_year = m["rev_by_year"]
    companies = m["companies"]

    current_year = datetime.now().year
    rev_current = float(rev_by_year.get(current_year, 0.0))

    c1, c2 = st.columns(2)

    with c1:
        st.markdown(
            f"""
            <div style="
                text-align:right;
                padding:8px 12px;
                border-radius:12px;
                background: linear-gradient(135deg, #8b2cff 0%, #5a22ff 45%, #a100ff 100%);
                color:#fff;
                font-family:system-ui, sans-serif;
                box-shadow: 0 8px 18px rgba(130, 46, 255, 0.25);
            ">
                <div style="font-size:12px; opacity:0.85; letter-spacing:0.2px;">Torches sold (total)</div>
                <div style="font-size:34px; font-weight:800; line-height:1;">{total_qty}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            f"""
            <div style="
                text-align:right;
                padding:8px 12px;
                border-radius:12px;
                background: linear-gradient(135deg, #00b894 0%, #0984e3 55%, #6c5ce7 100%);
                color:#fff;
                font-family:system-ui, sans-serif;
                box-shadow: 0 8px 18px rgba(9, 132, 227, 0.22);
            ">
                <div style="font-size:12px; opacity:0.85; letter-spacing:0.2px;">Revenue {current_year} (USD)</div>
                <div style="font-size:28px; font-weight:800; line-height:1;">${rev_current:,.0f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if rev_by_year:
        years_line = " • ".join([f"{y}: ${rev_by_year[y]:,.0f}" for y in sorted(rev_by_year.keys())])
        st.caption("Revenue by year: " + years_line)
    else:
        st.caption("Revenue by year: none yet")

    if companies:
        st.caption("Sold to: " + " • ".join(companies))
    else:
        st.caption("Sold to: no customers yet")


# -------------------------------------------------------------
# CONVERSION + SPEED METRICS (Contacted → Won)
# -------------------------------------------------------------
def get_first_status_times(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
          contact_id,
          MIN(CASE WHEN new_status='Contacted' THEN ts END) AS first_contacted_ts,
          MIN(CASE WHEN new_status='Won' THEN ts END) AS first_won_ts
        FROM status_history
        GROUP BY contact_id
        """,
        conn,
    )


def get_conversion_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    contacts = pd.read_sql_query("SELECT id, status, scan_datetime FROM contacts", conn)
    hist = get_first_status_times(conn)
    sales_agg = get_sales_agg(conn)[["contact_id", "first_sold_at"]].rename(columns={"contact_id": "id"})

    df = contacts.merge(hist, how="left", left_on="id", right_on="contact_id")
    df.drop(columns=["contact_id"], inplace=True, errors="ignore")
    df = df.merge(sales_agg, how="left", on="id")

    # counts as contacted if they ever hit Contacted, or their current status is beyond New
    df["status"] = df["status"].fillna("New").astype(str).str.strip()
    df["is_contacted"] = df["first_contacted_ts"].notna() | df["status"].isin(
        ["Contacted", "Meeting", "Quoted", "Won", "Lost", "Nurture", "Pending", "On hold", "Irrelevant"]
    )

    df_contacted = df[df["is_contacted"]].copy()

    # define win moment: prefer first sale date; else first time status became Won
    df_contacted["win_ts"] = df_contacted["first_sold_at"].fillna(df_contacted["first_won_ts"])

    contacted_n = int(df_contacted.shape[0])
    won_n = int(df_contacted["win_ts"].notna().sum())
    conv_rate = (won_n / contacted_n * 100.0) if contacted_n else 0.0

    # time to win (days): first_contacted_ts, fallback scan_datetime
    df_contacted["contact_ts"] = df_contacted["first_contacted_ts"].fillna(df_contacted["scan_datetime"])
    df_contacted["contact_ts"] = pd.to_datetime(df_contacted["contact_ts"], errors="coerce")
    df_contacted["win_ts"] = pd.to_datetime(df_contacted["win_ts"], errors="coerce")

    tt = df_contacted[df_contacted["contact_ts"].notna() & df_contacted["win_ts"].notna()].copy()
    tt["days_to_win"] = (tt["win_ts"] - tt["contact_ts"]).dt.total_seconds() / 86400.0
    tt = tt[tt["days_to_win"] >= 0]

    median_days = float(tt["days_to_win"].median()) if not tt.empty else None
    mean_days = float(tt["days_to_win"].mean()) if not tt.empty else None

    return {
        "contacted_n": contacted_n,
        "won_n": won_n,
        "conv_rate": conv_rate,
        "median_days": median_days,
        "mean_days": mean_days,
        "tt_df": tt[["id", "days_to_win"]].copy() if not tt.empty else pd.DataFrame(columns=["id", "days_to_win"]),
    }


def show_pipeline_stats(conn: sqlite3.Connection):
    st.subheader("📈 Pipeline performance")
    m = get_conversion_metrics(conn)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Contacted leads", m["contacted_n"])
    k2.metric("Won from contacted", m["won_n"])
    k3.metric("Contacted → Won", f"{m['conv_rate']:.1f}%")
    k4.metric("Median days to win", "—" if m["median_days"] is None else f"{m['median_days']:.1f}")

    if m["mean_days"] is not None:
        st.caption(f"Average days to win: {m['mean_days']:.1f}")

    if not m["tt_df"].empty:
        st.caption("Time-to-win (days) — by contact id")
        st.dataframe(m["tt_df"].sort_values("days_to_win"), use_container_width=True, hide_index=True)


# -------------------------------------------------------------
# SAFE DEDUPE
# -------------------------------------------------------------
def dedupe_database(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    try:
        cur.execute("DROP INDEX IF EXISTS idx_contacts_dedupe_key")
    except Exception:
        pass

    cur.execute("UPDATE contacts SET dedupe_key=NULL")
    conn.commit()

    rows = cur.execute(
        "SELECT id, first_name, last_name, company, email, profile_url FROM contacts"
    ).fetchall()

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
        ids = [r[0] for r in cur.execute(
            "SELECT id FROM contacts WHERE dedupe_key=? ORDER BY id ASC", (k,)
        ).fetchall()]

        if len(ids) <= 1:
            continue

        winner = ids[0]
        losers = ids[1:]

        for lose_id in losers:
            cur.execute("UPDATE notes SET contact_id=? WHERE contact_id=?", (winner, lose_id))
            cur.execute("UPDATE status_history SET contact_id=? WHERE contact_id=?", (winner, lose_id))
            cur.execute("UPDATE sales SET contact_id=? WHERE contact_id=?", (winner, lose_id))

        cur.execute(
            "DELETE FROM contacts WHERE id IN (" + ",".join("?" for _ in losers) + ")",
            losers
        )
        deleted += len(losers)

    conn.commit()

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

    backup_contacts(conn)
    return deleted


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
        return
    except Exception:
        dedupe_database(conn)
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
IND_PAT = re.compile(
    r"\b(director|manager|engineer|scientist|vp|founder|ceo|cto|lead|principal)\b",
    re.I,
)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {
        c: COLMAP.get(str(c).strip().lower(), str(c).strip().lower())
        for c in df.columns
    }
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


# -------------------------------------------------------------
# FLEXIBLE FILE LOADER
# -------------------------------------------------------------
def _fix_header_row_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    cols_lower = [str(c).strip().lower() for c in df.columns]
    if "first_name" in cols_lower or "first name" in cols_lower:
        return df
    if df.empty:
        return df

    first_row = df.iloc[0]
    first_vals = [
        "" if (isinstance(v, float) and pd.isna(v)) else str(v).strip()
        for v in first_row
    ]
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
def _find_existing_contact_id(cur: sqlite3.Cursor, dedupe_key: str, email: Optional[str], profile_url: Optional[str]) -> Optional[int]:
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
                        """
                        INSERT INTO status_history(contact_id, ts, old_status, new_status)
                        VALUES (?,?,?,?)
                        """,
                        (existing_id, datetime.utcnow().isoformat(), (existing_status or "New").strip(), (final_status or "New").strip()),
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
                      scan_datetime,
                      first_name,
                      last_name,
                      job_title,
                      company,
                      street,
                      street2,
                      zip_code,
                      city,
                      state,
                      country,
                      phone,
                      email,
                      website,
                      category,
                      status,
                      owner,
                      last_touch,
                      gender,
                      application,
                      product_interest,
                      photo,
                      profile_url,
                      dedupe_key
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
        """
        INSERT INTO status_history(contact_id, ts, old_status, new_status)
        VALUES (?,?,?,?)
        """,
        (contact_id, ts_iso, old_status, new_status),
    )
    cur.execute("UPDATE contacts SET status=?, last_touch=? WHERE id=?", (new_status, ts_iso, contact_id))
    conn.commit()
    backup_contacts(conn)


# -------------------------------------------------------------
# HOT / POTENTIAL / COLD OVERVIEW (HTML COMPONENTS)
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
            profile_icon_html = (
                f"<a href='{profile}' target='_blank' "
                f"style='text-decoration:none;font-size:16px;'>👤</a>"
            )
        else:
            profile_icon_html = "<span style='font-size:16px;'>👤</span>"

        status_badge = (
            f"<span style='padding:2px 8px;border-radius:999px;"
            f"background:rgba(255,255,255,0.10);font-size:12px;'>{status}</span>"
            if status else ""
        )

        rows_html.append(f"""
        <div style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.08);">
          <div style="flex:0 0 auto;margin-top:2px;">{profile_icon_html}</div>

          <div style="flex:1 1 auto;min-width:0;">
            <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;">
              <div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                {lead} {flag}
              </div>
              <div style="flex:0 0 auto;">{status_badge}</div>
            </div>

            <div style="font-size:12px;opacity:0.75;margin-top:2px;line-height:1.2;">
              {meta}
            </div>
          </div>
        </div>
        """)

    block = f"""
    <div style="font-family:system-ui, -apple-system, Segoe UI, Roboto, Arial;">
      {''.join(rows_html)}
    </div>
    """
    est_height = min(1200, 54 * len(df) + 60)
    components.html(block, height=est_height, scrolling=True)


def show_priority_lists(conn: sqlite3.Connection):
    st.subheader("Customer overview")

    df_all = pd.read_sql_query(
        "SELECT id, first_name, last_name, company, email, status, profile_url, country, product_interest, application FROM contacts",
        conn,
    )
    if df_all.empty:
        st.caption("No contacts yet – add someone manually or import a file.")
        return

    df_all["status"] = df_all["status"].fillna("New").astype(str).str.strip()

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

        if not hot_raw.empty:
            hot_options = {
                int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                for row in hot_raw.itertuples()
            }
            selected_hot = st.selectbox(
                "Pick hot lead to move",
                list(hot_options.keys()),
                format_func=lambda cid: hot_options.get(cid, str(cid)),
                key="hot_select",
            )
            c_hot1, c_hot2 = st.columns(2)
            with c_hot1:
                if st.button("Move to Potential", key="btn_hot_to_pot"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_hot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    new_status = "Contacted" if old == "Meeting" else ("Lost" if old == "Quoted" else old)
                    update_contact_status(conn, selected_hot, new_status)
                    st.rerun()
            with c_hot2:
                if st.button("Move to Cold", key="btn_hot_to_cold"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_hot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    new_status = "Pending" if old == "Meeting" else ("Lost" if old == "Quoted" else old)
                    update_contact_status(conn, selected_hot, new_status)
                    st.rerun()

    with col2:
        pot_header = f"""
            <div style="background-color:#28a745;padding:6px 10px;border-radius:10px;
                        font-weight:700;color:white;text-align:center;margin-bottom:6px;">
                🌱 Potential customers ({len(pot_raw)}) — New / Contacted
            </div>
        """
        _render_lead_list(pot_header, pot_raw)

        if not pot_raw.empty:
            pot_options = {
                int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                for row in pot_raw.itertuples()
            }
            selected_pot = st.selectbox(
                "Pick potential lead to move",
                list(pot_options.keys()),
                format_func=lambda cid: pot_options.get(cid, str(cid)),
                key="pot_select",
            )
            c_pot1, c_pot2 = st.columns(2)
            with c_pot1:
                if st.button("Move to Hot", key="btn_pot_to_hot"):
                    update_contact_status(conn, selected_pot, "Meeting")
                    st.rerun()
            with c_pot2:
                if st.button("Move to Cold", key="btn_pot_to_cold"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_pot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    new_status = "Irrelevant" if old == "New" else ("Pending" if old == "Contacted" else old)
                    update_contact_status(conn, selected_pot, new_status)
                    st.rerun()

    with col3:
        cold_header = f"""
            <div style="background-color:#007bff;padding:6px 10px;border-radius:10px;
                        font-weight:700;color:white;text-align:center;margin-bottom:6px;">
                ❄️ Cold customers ({len(cold_raw)}) — Pending / On hold / Irrelevant
            </div>
        """
        _render_lead_list(cold_header, cold_raw)

        if not cold_raw.empty:
            cold_options = {
                int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                for row in cold_raw.itertuples()
            }
            selected_cold = st.selectbox(
                "Pick cold lead to move",
                list(cold_options.keys()),
                format_func=lambda cid: cold_options.get(cid, str(cid)),
                key="cold_select",
            )
            c_cold1, c_cold2 = st.columns(2)
            with c_cold1:
                if st.button("Move to Potential", key="btn_cold_to_pot"):
                    update_contact_status(conn, selected_cold, "Contacted")
                    st.rerun()
            with c_cold2:
                if st.button("Move to Hot", key="btn_cold_to_hot"):
                    update_contact_status(conn, selected_cold, "Meeting")
                    st.rerun()


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
    st.sidebar.caption(f"Total contacts: **{total}**")

    export_df = st.session_state.get("export_df")
    if isinstance(export_df, pd.DataFrame) and not export_df.empty:
        csv_bytes = export_df.to_csv(index=False, quoting=csv.QUOTE_ALL).encode("utf-8")
        st.sidebar.download_button("Download Contacts CSV (filtered)", csv_bytes, file_name="radom-contacts.csv")


# -------------------------------------------------------------
# FILTERS UI
# -------------------------------------------------------------
def filters_ui():
    st.subheader("Filters")
    q = st.text_input("Search (name, email, company)", "")

    c1, c2, c3 = st.columns(3)
    with c1:
        cats = st.multiselect("Category", ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"], [])
    with c2:
        stats = st.multiselect("Status", PIPELINE, [])
    with c3:
        st_like = st.text_input("State/Province contains", "")

    c4, c5 = st.columns(2)
    with c4:
        app_filter = st.multiselect("Application", APPLICATIONS, [])
    with c5:
        prod_filter = st.multiselect("Product type interest", PRODUCTS, [])

    return q, cats, stats, st_like, app_filter, prod_filter


# -------------------------------------------------------------
# CONTACT EDITOR + NOTES + SALES
# -------------------------------------------------------------
def contact_editor(conn: sqlite3.Connection, row: pd.Series):
    st.markdown("---")

    contact_id = int(row["id"])
    st.markdown(f"### ✏️ {row.get('first_name','')} {row.get('last_name','')} — {row.get('company') or ''}")
    st.caption(f"Status: {row.get('status') or 'New'} | Application: {row.get('application') or '—'}")

    profile_url_header = row.get("profile_url")
    if profile_url_header:
        u = _clean_url(profile_url_header)
        st.markdown(f"🔗 Profile: [{u}]({u})")

    website_header = row.get("website")
    if website_header:
        w = _clean_url(website_header)
        st.markdown(f"🌐 Website: [{w}]({w})")

    old_status_outer = (row.get("status") or "New").strip()

    with st.form(f"edit_{contact_id}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            first = st.text_input("First name", row.get("first_name") or "")
            job = st.text_input("Job title", row.get("job_title") or "")
            phone = st.text_input("Phone", row.get("phone") or "")

            gender_options = ["", "Female", "Male", "Other"]
            raw_gender = row.get("gender") or ""
            gender = st.selectbox("Gender", gender_options, index=gender_options.index(raw_gender) if raw_gender in gender_options else 0)

        with col2:
            last = st.text_input("Last name", row.get("last_name") or "")
            company = st.text_input("Company", row.get("company") or "")
            email = st.text_input("Email", row.get("email") or "")
            app_options = [""] + APPLICATIONS
            current_app = row.get("application") or ""
            app_index = app_options.index(current_app) if current_app in app_options else 0
            application = st.selectbox("Application", app_options, index=app_index)

        with col3:
            cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
            category = st.selectbox("Category", cat_opts, index=cat_opts.index(row["category"]) if row.get("category") in cat_opts else 0)
            status = st.selectbox("Status", PIPELINE, index=PIPELINE.index(row["status"]) if row.get("status") in PIPELINE else 0)

            product_options = [""] + PRODUCTS
            raw_prod = row.get("product_interest") or ""
            prod_index = product_options.index(raw_prod) if raw_prod in product_options else 0
            product = st.selectbox("Product type interest", product_options, index=prod_index)

            raw_owner = row.get("owner") or ""
            owner_index = OWNERS.index(raw_owner) if raw_owner in OWNERS else 0
            owner = st.selectbox("Owner", OWNERS, index=owner_index)

        st.write("**Address**")
        street = st.text_input("Street", row.get("street") or "")
        street2 = st.text_input("Street 2", row.get("street2") or "")
        city = st.text_input("City", row.get("city") or "")
        state = st.text_input("State/Province", row.get("state") or "")
        zipc = st.text_input("ZIP", row.get("zip_code") or "")
        country = st.text_input("Country", row.get("country") or "")
        website = st.text_input("Website", row.get("website") or "")
        profile_url = st.text_input("Profile URL", row.get("profile_url") or "")

        # ✅ If status becomes Won, require sales lines
        becoming_won = (old_status_outer != "Won" and (status or "").strip() == "Won")
        sale_lines: List[Dict[str, Any]] = []
        sold_date: Optional[date] = None

        if becoming_won:
            st.markdown("### 💰 Sale details (required for Won)")
            sold_date = st.date_input("Sale date", value=date.today(), key=f"sold_date_{contact_id}")
            st.caption("Add the torches sold in this deal (product + qty + unit price).")

            for i in range(1, 5):
                cA, cB, cC = st.columns([2, 1, 1])
                with cA:
                    prod = st.selectbox(
                        f"Product #{i}",
                        [""] + PRODUCTS,
                        key=f"sale_prod_{contact_id}_{i}",
                    )
                with cB:
                    qty = st.number_input(
                        f"Qty #{i}",
                        min_value=0,
                        step=1,
                        value=0,
                        key=f"sale_qty_{contact_id}_{i}",
                    )
                with cC:
                    price = st.number_input(
                        f"Unit price (USD) #{i}",
                        min_value=0.0,
                        step=1000.0,
                        value=0.0,
                        key=f"sale_price_{contact_id}_{i}",
                    )

                if prod and int(qty) > 0:
                    sale_lines.append({"product": prod, "qty": int(qty), "unit_price": float(price)})

        col_save, col_delete = st.columns([3, 1])
        saved = col_save.form_submit_button("Save changes")
        delete_pressed = col_delete.form_submit_button("🗑️ Delete this contact")

        if saved:
            cur = conn.cursor()
            old_status = old_status_outer
            new_status_norm = (status or "New").strip()

            if old_status != "Won" and new_status_norm == "Won":
                if not sale_lines:
                    st.error("To set status to Won, please enter at least one sold torch line (product + qty + price).")
                    st.stop()

            if old_status != new_status_norm:
                cur.execute(
                    "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
                    (contact_id, datetime.utcnow().isoformat(), old_status, new_status_norm),
                )

            ts_iso = datetime.utcnow().isoformat()
            email_norm = (_norm_email(email) or None)
            profile_norm = _clean_url(profile_url) or None

            dedupe_key = compute_dedupe_key(first, last, company, email_norm, profile_norm) or None

            conn.execute(
                """
                UPDATE contacts SET
                    first_name=?, last_name=?, job_title=?, company=?, phone=?, email=?,
                    category=?, status=?, owner=?, street=?, street2=?, city=?, state=?,
                    zip_code=?, country=?, website=?, profile_url=?, last_touch=?, gender=?, application=?, product_interest=?,
                    dedupe_key=?
                WHERE id=?
                """,
                (
                    first.strip() or None,
                    last.strip() or None,
                    job.strip() or None,
                    company.strip() or None,
                    phone.strip() or None,
                    email_norm,
                    category,
                    new_status_norm,
                    (owner or "").strip() or None,
                    street.strip() or None,
                    street2.strip() or None,
                    city.strip() or None,
                    state.strip() or None,
                    zipc.strip() or None,
                    country.strip() or None,
                    _clean_url(website) or None,
                    profile_norm,
                    ts_iso,
                    gender or None,
                    normalize_application(application),
                    (product or "").strip() or None,
                    dedupe_key,
                    contact_id,
                ),
            )
            conn.commit()

            # ✅ Record sales exactly when becoming Won
            if old_status != "Won" and new_status_norm == "Won":
                add_sales_lines(conn, contact_id, sold_date or date.today(), sale_lines, currency="USD")

            backup_contacts(conn)
            ensure_dedupe_index(conn)
            st.success("Saved")
            st.rerun()

        if delete_pressed:
            conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
            conn.commit()
            backup_contacts(conn)
            st.success("Contact deleted")
            st.rerun()

    st.markdown("#### 🗒️ Notes")
    note_key = f"note_{contact_id}"
    fu_key = f"nextfu_{contact_id}"

    new_note = st.text_area("Add a note", key=note_key, placeholder="Called; left voicemail…")
    next_fu = st.date_input("Next follow-up", value=st.session_state.get(fu_key, date.today()), key=fu_key)

    col_add_note, col_clear_note = st.columns([2, 1])
    with col_add_note:
        if st.button("Add note", key=f"addnote_{contact_id}"):
            if new_note.strip():
                cleaned = sanitize_note_text(new_note, trim_email_threads=False)
                ts_iso = datetime.utcnow().isoformat()
                fu_iso = next_fu.isoformat() if isinstance(next_fu, date) else None
                conn.execute(
                    "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                    (contact_id, ts_iso, cleaned, fu_iso),
                )
                conn.execute("UPDATE contacts SET last_touch=? WHERE id=?", (ts_iso, contact_id))
                conn.commit()
                backup_contacts(conn)
                st.session_state.pop(note_key, None)
                st.session_state.pop(fu_key, None)
                st.success("Note added")
                st.rerun()

    with col_clear_note:
        if st.button("Clear note", key=f"clearnote_{contact_id}"):
            st.session_state.pop(note_key, None)
            st.session_state.pop(fu_key, None)
            st.rerun()

    notes_df = get_notes(conn, contact_id)
    st.dataframe(notes_df, use_container_width=True)

    # ✅ Sales history display
    st.markdown("#### 🧾 Sales history")
    sales_df = pd.read_sql_query(
        """
        SELECT sold_at, year, product, qty,
               unit_price_cents/100.0 AS unit_price_usd,
               (qty * unit_price_cents)/100.0 AS line_total_usd
        FROM sales
        WHERE contact_id=?
        ORDER BY sold_at DESC, id DESC
        """,
        conn,
        params=(contact_id,),
    )
    st.dataframe(sales_df, use_container_width=True, hide_index=True)


# -------------------------------------------------------------
# MANUAL ADD CONTACT FORM (NO DUPLICATES)
# -------------------------------------------------------------
def add_contact_form(conn: sqlite3.Connection):
    st.markdown("### ➕ Add new contact manually")

    st.session_state.setdefault("add_form_reset", 0)
    rid = st.session_state["add_form_reset"]

    def k(name: str) -> str:
        return f"{name}_{rid}"

    with st.expander("Open form"):
        with st.form(f"add_contact_form_{rid}"):
            col1, col2, col3 = st.columns(3)

            with col1:
                first = st.text_input("First name", key=k("add_first"))
                job = st.text_input("Job title", key=k("add_job"))
                phone = st.text_input("Phone", key=k("add_phone"))
                gender = st.selectbox("Gender", ["", "Female", "Male", "Other"], key=k("add_gender"))

            with col2:
                last = st.text_input("Last name", key=k("add_last"))
                company = st.text_input("Company", key=k("add_company"))
                email = st.text_input("Email", key=k("add_email"))
                application_raw = st.selectbox("Application", [""] + APPLICATIONS, key=k("add_application"))

            with col3:
                cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
                category = st.selectbox("Category", cat_opts, index=3, key=k("add_category"))
                status = st.selectbox("Status", PIPELINE, index=0, key=k("add_status"))
                owner = st.selectbox("Owner", OWNERS, key=k("add_owner"))
                product = st.selectbox("Product type interest", [""] + PRODUCTS, key=k("add_product"))

            st.write("**Address**")
            street = st.text_input("Street", key=k("add_street"))
            street2 = st.text_input("Street 2", key=k("add_street2"))
            city = st.text_input("City", key=k("add_city"))
            state = st.text_input("State/Province", key=k("add_state"))
            zipc = st.text_input("ZIP", key=k("add_zip"))
            country = st.text_input("Country", key=k("add_country"))
            website = st.text_input("Website", key=k("add_website"))
            profile_url = st.text_input("Profile URL", key=k("add_profile_url"))

            st.write("**Optional note (saved immediately)**")
            first_note = st.text_area("Note", key=k("add_note"), placeholder="Met at conference…")

            col_create, col_clear = st.columns([3, 1])
            submitted = col_create.form_submit_button("Create contact")
            cleared = col_clear.form_submit_button("Clear form")

        if cleared:
            st.session_state["add_form_reset"] += 1
            st.rerun()

        if submitted:
            email_norm = (_norm_email(email) or None)
            profile_norm = _clean_url(profile_url) or None

            if not email_norm and not (first and last and company):
                st.error("Please provide either an email, or first name + last name + company.")
                return

            scan_dt = datetime.utcnow().isoformat()
            status_norm = normalize_status(status) or "New"
            application_norm = normalize_application(application_raw)

            dedupe_key = compute_dedupe_key(first, last, company, email_norm, profile_norm) or None

            cur = conn.cursor()
            existing_id = _find_existing_contact_id(cur, dedupe_key or "", email_norm, profile_norm)

            cleaned_first_note = sanitize_note_text(first_note, trim_email_threads=False) if (first_note or "").strip() else ""

            if existing_id:
                conn.execute(
                    """
                    UPDATE contacts SET
                      scan_datetime=?, first_name=?, last_name=?, job_title=?, company=?,
                      street=?, street2=?, zip_code=?, city=?, state=?, country=?,
                      phone=?, email=?, website=?, category=?, status=?, owner=?, last_touch=?,
                      gender=?, application=?, product_interest=?, profile_url=?, dedupe_key=?
                    WHERE id=?
                    """,
                    (
                        scan_dt,
                        (first or "").strip() or None,
                        (last or "").strip() or None,
                        (job or "").strip() or None,
                        (company or "").strip() or None,
                        (street or "").strip() or None,
                        (street2 or "").strip() or None,
                        (zipc or "").strip() or None,
                        (city or "").strip() or None,
                        (state or "").strip() or None,
                        (country or "").strip() or None,
                        (phone or "").strip() or None,
                        email_norm,
                        _clean_url(website) or None,
                        category,
                        status_norm,
                        (owner or "").strip() or None,
                        scan_dt,
                        (gender or "").strip() or None,
                        application_norm,
                        (product or "").strip() or None,
                        profile_norm,
                        dedupe_key,
                        int(existing_id),
                    ),
                )

                if cleaned_first_note:
                    conn.execute(
                        "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                        (int(existing_id), scan_dt, cleaned_first_note, None),
                    )

                conn.commit()
                backup_contacts(conn)
                ensure_dedupe_index(conn)
                st.success("Contact already existed — updated it (no duplicate created).")
                st.session_state["add_form_reset"] += 1
                st.rerun()
                return

            conn.execute(
                """
                INSERT INTO contacts
                (scan_datetime, first_name, last_name, job_title, company,
                 street, street2, zip_code, city, state, country,
                 phone, email, website, category, status, owner, last_touch,
                 gender, application, product_interest, profile_url, dedupe_key)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    scan_dt,
                    (first or "").strip() or None,
                    (last or "").strip() or None,
                    (job or "").strip() or None,
                    (company or "").strip() or None,
                    (street or "").strip() or None,
                    (street2 or "").strip() or None,
                    (zipc or "").strip() or None,
                    (city or "").strip() or None,
                    (state or "").strip() or None,
                    (country or "").strip() or None,
                    (phone or "").strip() or None,
                    email_norm,
                    _clean_url(website) or None,
                    category,
                    status_norm,
                    (owner or "").strip() or None,
                    scan_dt,
                    (gender or "").strip() or None,
                    application_norm,
                    (product or "").strip() or None,
                    profile_norm,
                    dedupe_key,
                ),
            )
            contact_id = conn.execute("SELECT id FROM contacts WHERE rowid=last_insert_rowid()").fetchone()[0]

            if cleaned_first_note:
                conn.execute(
                    "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                    (int(contact_id), scan_dt, cleaned_first_note, None),
                )

            conn.commit()
            backup_contacts(conn)
            ensure_dedupe_index(conn)
            st.success("New contact created")
            st.session_state["add_form_reset"] += 1
            st.rerun()


# -------------------------------------------------------------
# MAIN APP
# -------------------------------------------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")

    inject_christmas_background()
    check_login_two_factor_telegram()

    conn = get_conn()
    init_db(conn)
    restore_from_backup_if_empty(conn)
    ensure_dedupe_index(conn)

    top_l, top_r = st.columns([3, 1])
    with top_l:
        st.title(APP_TITLE)
        st.caption("Upload leads → categorize → work the pipeline → export.")
    with top_r:
        show_sales_counters(conn)

    sidebar_import_export(conn)

    show_pipeline_stats(conn)
    show_priority_lists(conn)

    add_contact_form(conn)

    q, cats, stats, st_like, app_filter, prod_filter = filters_ui()
    df = query_contacts(conn, q, cats, stats, st_like, app_filter, prod_filter)

    notes_agg = get_notes_agg(conn)
    if not notes_agg.empty:
        df = df.merge(notes_agg, how="left", left_on="id", right_on="contact_id")
        df.drop(columns=["contact_id"], inplace=True, errors="ignore")
    if "notes" not in df.columns:
        df["notes"] = None

    if df.empty:
        st.info("No contacts match your filters or the database is empty. Add a contact or upload a file.")
        return

    export_cols = [
        "first_name",
        "last_name",
        "email",
        "phone",
        "job_title",
        "company",
        "city",
        "state",
        "country",
        "category",
        "status",
        "owner",
        "gender",
        "application",
        "product_interest",
        "website",
        "profile_url",
        "last_touch",
        "notes",
    ]
    available_cols = [c for c in export_cols if c in df.columns]

    # ✅ Build export with internal id, then attach sales aggregates, then drop id
    export_internal = df[["id"] + available_cols].copy()

    if "notes" in export_internal.columns:
        export_internal["notes"] = export_internal["notes"].apply(lambda x: sanitize_note_text(x, trim_email_threads=False))

    sales_agg = get_sales_agg(conn)
    if not sales_agg.empty:
        export_internal = export_internal.merge(sales_agg, how="left", left_on="id", right_on="contact_id")
        export_internal.drop(columns=["contact_id"], inplace=True, errors="ignore")

    # ensure sales columns exist
    for c in ["sold_qty", "sold_revenue_usd", "first_sold_at", "last_sold_at", "sales_lines"]:
        if c not in export_internal.columns:
            export_internal[c] = ""

    export_internal["sold_qty"] = export_internal["sold_qty"].fillna(0).astype(int)
    export_internal["sold_revenue_usd"] = export_internal["sold_revenue_usd"].fillna(0.0)
    export_internal["first_sold_at"] = export_internal["first_sold_at"].fillna("")
    export_internal["last_sold_at"] = export_internal["last_sold_at"].fillna("")
    export_internal["sales_lines"] = export_internal["sales_lines"].fillna("")

    export_df = export_internal.drop(columns=["id"], errors="ignore")
    st.session_state["export_df"] = export_df

    # Display table
    display_df = export_df.copy()
    if "profile_url" in display_df.columns:
        display_df = display_df.rename(columns={"profile_url": "Profile URL"})
    st.subheader("Contacts")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Contact picker still uses df with id
    options = [
        (int(r.id), f"{(r.first_name or '').strip()} {(r.last_name or '').strip()} — {(r.company or '').strip()}")
        for r in df[["id", "first_name", "last_name", "company"]].itertuples(index=False)
    ]
    option_labels = {opt[0]: opt[1] for opt in options}

    chosen_id = st.selectbox(
        "Select a contact to edit",
        [opt[0] for opt in options],
        format_func=lambda x: option_labels.get(x, str(x)),
    )

    if chosen_id:
        row = df[df["id"] == chosen_id].iloc[0]
        contact_editor(conn, row)


if __name__ == "__main__":
    main()
