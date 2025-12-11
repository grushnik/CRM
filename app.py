import os
import re
import sqlite3
import random
import time
from datetime import datetime, date
from typing import List, Any, Optional, Dict, Tuple

import pandas as pd
import streamlit as st
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

# Team owners list (dropdown)
OWNER_OPTIONS = ["", "Velibor", "Liz", "Jovan", "Ian", "Qi", "Kenshin"]

# Raw application list, then sorted alphabetically for UI
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

# -------------------------------------------------------------
# TELEGRAM OTP (2-FACTOR)
# Multi-user support by username -> chat_id via getUpdates caching
# -------------------------------------------------------------
def _telegram_api(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


def _telegram_get_updates(token: str) -> Dict[str, Any]:
    # NOTE: works even if you don't run a bot server; Telegram stores updates.
    # This only helps AFTER the user has pressed Start / sent any message to the bot once.
    url = _telegram_api(token, "getUpdates")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _detect_chat_id_for_username(token: str, username_no_at: str) -> Optional[int]:
    """
    Search recent updates for a message from this username.
    Return chat.id if found, else None.
    """
    if not username_no_at:
        return None
    u = username_no_at.strip().lstrip("@").lower()

    try:
        data = _telegram_get_updates(token)
        if not data.get("ok"):
            return None
        for upd in reversed(data.get("result", [])):
            msg = upd.get("message") or upd.get("edited_message") or {}
            frm = msg.get("from") or {}
            chat = msg.get("chat") or {}
            uname = (frm.get("username") or "").lower()
            if uname == u:
                cid = chat.get("id")
                if isinstance(cid, int):
                    return cid
    except Exception:
        return None

    return None


def _send_telegram_message(token: str, chat_id: int, text: str) -> Tuple[bool, str]:
    url = _telegram_api(token, "sendMessage")
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        if resp.status_code == 200:
            return True, ""
        return False, f"Status {resp.status_code}: {resp.text}"
    except Exception as e:
        return False, str(e)


def _send_telegram_otp_to_user(username_no_at: str, code: str) -> Tuple[bool, str]:
    token = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return False, "TELEGRAM_BOT_TOKEN is missing in secrets."

    # Prefer explicit ADMIN_CHAT_ID if username matches admin
    admin_username = (st.secrets.get("ADMIN_USERNAME") or "").strip().lstrip("@").lower()
    admin_chat_id = st.secrets.get("ADMIN_CHAT_ID")
    requested_username = username_no_at.strip().lstrip("@").lower()

    chat_id = None
    if admin_chat_id and admin_username and requested_username == admin_username:
        try:
            chat_id = int(admin_chat_id)
        except Exception:
            chat_id = None

    # Try auto-detect from getUpdates
    if chat_id is None:
        chat_id = _detect_chat_id_for_username(token, username_no_at)

    if chat_id is None:
        return False, (
            "Could not detect your Telegram chat. Open Telegram, search for the bot, "
            "press Start (or send any message), then try again."
        )

    text = f"🔐 Radom CRM login code: {code} (valid {OTP_TTL_SECONDS//60} min)"
    ok, err = _send_telegram_message(token, int(chat_id), text)
    if not ok:
        return False, f"Failed to send Telegram message. {err}"

    return True, ""


def check_login_two_factor_telegram():
    expected_pw = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)

    ss = st.session_state
    ss.setdefault("auth_pw_ok", False)
    ss.setdefault("authed", False)
    ss.setdefault("otp_sent_ok", False)
    ss.setdefault("otp_error", "")
    ss.setdefault("tg_username", "")

    if ss["authed"]:
        return

    st.sidebar.header("🔐 Login")

    # User requested order: username first, then password
    username = st.sidebar.text_input("Telegram username (without @)", value=ss.get("tg_username", ""))
    ss["tg_username"] = username.strip()

    pwd = st.sidebar.text_input("Password", type="password")

    if st.sidebar.button("Continue"):
        if pwd != expected_pw:
            st.sidebar.error("Wrong password")
            st.stop()

        # password ok -> generate OTP and try Telegram
        code = f"{random.randint(0, 999999):06d}"
        ss["otp_code"] = code
        ss["otp_time"] = int(time.time())

        ok, err = _send_telegram_otp_to_user(ss["tg_username"], code)
        ss["otp_sent_ok"] = ok
        ss["otp_error"] = err

        st.rerun()

    # Not continued yet
    if "otp_time" not in ss:
        st.stop()

    # OTP TTL
    if int(time.time()) - ss.get("otp_time", 0) > OTP_TTL_SECONDS:
        for k in ("auth_pw_ok", "otp_code", "otp_time", "otp_sent_ok", "otp_error"):
            ss.pop(k, None)
        st.sidebar.error("Code expired. Please start over.")
        st.stop()

    # If Telegram failed, show a troubleshooting expander and ONLY THEN show a backup code
    with st.sidebar.expander("Troubleshooting", expanded=not ss.get("otp_sent_ok", False)):
        if ss.get("otp_sent_ok"):
            st.success("OTP was sent to Telegram.")
        else:
            st.error(ss.get("otp_error") or "Failed to send OTP.")
            st.info(f"Use this one-time code instead: **{ss.get('otp_code','')}**")

        # Optional: test telegram button
        if st.button("🧪 Test Telegram", key="tg_test"):
            token = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
            admin_chat_id = st.secrets.get("ADMIN_CHAT_ID")
            if not (token and admin_chat_id):
                st.warning("Set TELEGRAM_BOT_TOKEN and ADMIN_CHAT_ID in secrets.")
            else:
                ok, err = _send_telegram_message(
                    token, int(admin_chat_id), "✅ Telegram test from Radom CRM (admin_chat_id)"
                )
                if ok:
                    st.success("Test message sent. Check your Telegram chat with the bot.")
                else:
                    st.error(f"Test failed: {err}")

    st.sidebar.write("Enter the 6-digit code sent to Telegram (or the backup code if shown above).")
    code_in = st.sidebar.text_input("Enter 6-digit code", max_chars=6)

    if st.sidebar.button("Verify"):
        if code_in.strip() == ss.get("otp_code", ""):
            ss["authed"] = True
            for k in ("auth_pw_ok", "otp_code", "otp_time", "otp_sent_ok", "otp_error"):
                ss.pop(k, None)
            st.rerun()
        else:
            st.sidebar.error("Incorrect code")
            st.stop()

    st.stop()


# -------------------------------------------------------------
# DB + BACKUP HELPERS
# -------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, coltype: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")


def init_db(conn: sqlite3.Connection):
    # Create tables if missing (fresh install)
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
          profile_url TEXT
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
        """
    )

    # Auto-migrate older DBs safely (THIS fixes your import crash)
    _ensure_column(conn, "contacts", "owner", "TEXT")
    _ensure_column(conn, "contacts", "last_touch", "TEXT")
    _ensure_column(conn, "contacts", "gender", "TEXT")
    _ensure_column(conn, "contacts", "application", "TEXT")
    _ensure_column(conn, "contacts", "product_interest", "TEXT")
    _ensure_column(conn, "contacts", "photo", "TEXT")
    _ensure_column(conn, "contacts", "profile_url", "TEXT")
    _ensure_column(conn, "contacts", "website", "TEXT")
    _ensure_column(conn, "contacts", "country", "TEXT")

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
# IMPORT / NORMALIZATION
# -------------------------------------------------------------
COLMAP = {
    "scan date/time": "scan_datetime",
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
    # profile links
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
    first_vals = ["" if (isinstance(v, float) and pd.isna(v)) else str(v).strip() for v in first_row]
    first_vals_lower = [v.lower() for v in first_vals]

    known = set(COLMAP.keys()) | set(EXPECTED) | {"first_name", "last_name", "email", "company"}
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
    df = _fix_header_row_if_needed(df)
    return df


def upsert_contacts(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    df = normalize_columns(df).fillna("")
    df["category"] = df.apply(infer_category, axis=1)
    df["scan_datetime"] = df["scan_datetime"].apply(parse_dt)
    df["status_norm"] = df.get("status", "").apply(normalize_status)

    n = 0
    cur = conn.cursor()

    for idx, r in df.iterrows():
        email = (r.get("email") or "").strip().lower() or None
        status_from_file = r.get("status_norm")
        note_text = (r.get("notes") or "").strip()
        photo_path = (r.get("photo") or "").strip() or None
        profile_url = (r.get("profile_url") or "").strip() or None
        website = (r.get("website") or "").strip() or None
        owner = (r.get("owner") or "").strip() or None
        last_touch = parse_dt(r.get("last_touch")) or None

        try:
            if email:
                cur.execute("SELECT id, status FROM contacts WHERE email=?", (email,))
                row = cur.fetchone()
            else:
                cur.execute(
                    "SELECT id, status FROM contacts WHERE first_name=? AND last_name=? AND company=?",
                    (r["first_name"], r["last_name"], r["company"]),
                )
                row = cur.fetchone()

            existing_id = row[0] if row else None
            existing_status = row[1] if row and len(row) > 1 else None
            final_status = status_from_file or existing_status or "New"

            gender = (r.get("gender") or "").strip() or None
            application = normalize_application(r.get("application"))
            product_interest = (r.get("product_interest") or "").strip() or None

            payload_common = (
                r["scan_datetime"],
                r["first_name"],
                r["last_name"],
                r["job_title"],
                r["company"],
                r["street"],
                r["street2"],
                r["zip_code"],
                r["city"],
                r["state"],
                r["country"],
                str(r["phone"]) if str(r.get("phone","")).strip() else None,
                email,
                website,
                r["category"],
            )

            if existing_id:
                if existing_status != final_status:
                    cur.execute(
                        """
                        INSERT INTO status_history(contact_id, ts, old_status, new_status)
                        VALUES (?,?,?,?)
                        """,
                        (
                            existing_id,
                            datetime.utcnow().isoformat(),
                            existing_status,
                            final_status,
                        ),
                    )
                cur.execute(
                    """
                    UPDATE contacts SET
                        scan_datetime=?, first_name=?, last_name=?, job_title=?, company=?,
                        street=?, street2=?, zip_code=?, city=?, state=?, country=?, phone=?, email=?, website=?,
                        category=?, status=?, owner=?, last_touch=?, gender=?, application=?, product_interest=?, photo=?, profile_url=?
                    WHERE id=?
                    """,
                    payload_common
                    + (
                        final_status,
                        owner,
                        last_touch or datetime.utcnow().isoformat(),
                        gender,
                        application,
                        product_interest,
                        photo_path,
                        profile_url,
                        existing_id,
                    ),
                )
                contact_id = existing_id
            else:
                cur.execute(
                    """
                    INSERT INTO contacts
                    (scan_datetime, first_name, last_name, job_title, company, street, street2, zip_code,
                     city, state, country, phone, email, website, category, status, owner, last_touch, gender, application,
                     product_interest, photo, profile_url)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    payload_common
                    + (
                        final_status,
                        owner,
                        last_touch or datetime.utcnow().isoformat(),
                        gender,
                        application,
                        product_interest,
                        photo_path,
                        profile_url,
                    ),
                )
                contact_id = cur.lastrowid

            if note_text:
                ts_iso = r["scan_datetime"] or datetime.utcnow().isoformat()
                cur.execute(
                    "SELECT 1 FROM notes WHERE contact_id=? AND body=?",
                    (contact_id, note_text),
                )
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                        (contact_id, ts_iso, note_text, None),
                    )

            n += 1

        except sqlite3.Error as e:
            st.error(
                f"Database error on row {idx + 1} "
                f"(email='{email}', name='{r.get('first_name')} {r.get('last_name')}'): {e}"
            )
            continue

    conn.commit()
    backup_contacts(conn)
    return n


# -------------------------------------------------------------
# QUERIES & STATUS UPDATE
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
    df = pd.read_sql_query(
        "SELECT contact_id, ts, body FROM notes ORDER BY contact_id, ts", conn
    )
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
    cur.execute(
        "UPDATE contacts SET status=?, last_touch=? WHERE id=?",
        (new_status, ts_iso, contact_id),
    )
    conn.commit()
    backup_contacts(conn)


# -------------------------------------------------------------
# FLAGS + LINK HELPERS
# -------------------------------------------------------------
_COUNTRY_TO_ISO2 = {
    "united states": "US",
    "usa": "US",
    "u.s.a.": "US",
    "us": "US",
    "canada": "CA",
    "mexico": "MX",
    "colombia": "CO",
    "chile": "CL",
    "brazil": "BR",
    "argentina": "AR",
    "united kingdom": "GB",
    "uk": "GB",
    "england": "GB",
    "germany": "DE",
    "france": "FR",
    "italy": "IT",
    "spain": "ES",
    "czech republic": "CZ",
    "czechia": "CZ",
    "poland": "PL",
    "sweden": "SE",
    "norway": "NO",
    "finland": "FI",
    "switzerland": "CH",
    "austria": "AT",
    "netherlands": "NL",
    "belgium": "BE",
    "ireland": "IE",
    "india": "IN",
    "china": "CN",
    "japan": "JP",
    "south korea": "KR",
    "korea": "KR",
    "taiwan": "TW",
    "singapore": "SG",
    "australia": "AU",
    "new zealand": "NZ",
    "saudi arabia": "SA",
    "united arab emirates": "AE",
    "uae": "AE",
    "turkey": "TR",
    "israel": "IL",
}


def _flag_emoji_from_country(country: Any) -> str:
    if country is None:
        return ""
    s = str(country).strip()
    if not s:
        return ""
    c = s.lower()
    iso2 = ""
    if len(s) == 2 and s.isalpha():
        iso2 = s.upper()
    else:
        iso2 = _COUNTRY_TO_ISO2.get(c, "")
    if not iso2 or len(iso2) != 2:
        return ""
    # Convert ISO2 to flag emoji
    return chr(127397 + ord(iso2[0])) + chr(127397 + ord(iso2[1]))


def _clean_url(u: Any) -> str:
    if u is None:
        return ""
    s = str(u).strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return "https://" + s


# -------------------------------------------------------------
# WON COUNTER
# -------------------------------------------------------------
def show_won_counter(conn: sqlite3.Connection):
    df_count = pd.read_sql_query(
        "SELECT COUNT(*) AS n FROM contacts WHERE status='Won'", conn
    )
    n = int(df_count.iloc[0]["n"]) if not df_count.empty else 0

    df_companies = pd.read_sql_query(
        """
        SELECT DISTINCT TRIM(company) AS company
        FROM contacts
        WHERE status='Won' AND company IS NOT NULL AND TRIM(company) <> ''
        ORDER BY company
        """,
        conn,
    )
    companies = df_companies["company"].dropna().tolist() if not df_companies.empty else []

    st.markdown(
        f"""
        <div style="
            text-align:right;
            padding:6px 10px;
            border-radius:8px;
            background-color:#222;
            color:#fff;
            font-family:system-ui, sans-serif;
        ">
            <div style="font-size:12px; opacity:0.7;">Sold systems</div>
            <div style="font-size:32px; font-weight:700;">{n}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if companies:
        st.caption("Sold to: " + " • ".join(companies))
    else:
        st.caption("Sold to: no customers yet")


# -------------------------------------------------------------
# TOP PRIORITY LISTS (HOT / POTENTIAL / COLD) with FLAGS + LINKS
# -------------------------------------------------------------
def show_priority_lists(conn: sqlite3.Connection):
    st.subheader("Customer overview")

    df_all = pd.read_sql_query(
        "SELECT id, first_name, last_name, company, email, status, profile_url, website, country FROM contacts",
        conn,
    )

    if df_all.empty:
        st.caption("No contacts yet – add someone manually or import a file.")
        return

    df_all["status"] = df_all["status"].fillna("New").astype(str).str.strip()

    def build_group(mask):
        sub = df_all[mask].copy()
        cols = ["Profile", "Website", "Lead", "Company", "Email", "Status"]
        if sub.empty:
            return sub, pd.DataFrame(columns=cols)

        sub["Lead"] = (
            sub["first_name"].fillna("") + " " + sub["last_name"].fillna("")
        ).str.strip()

        sub["Flag"] = sub["country"].apply(_flag_emoji_from_country)
        sub["Lead"] = sub["Lead"] + "  " + sub["Flag"]

        display = pd.DataFrame(
            {
                "Profile": sub.get("profile_url", "").fillna("").apply(_clean_url),
                "Website": sub.get("website", "").fillna("").apply(_clean_url),
                "Lead": sub["Lead"],
                "Company": sub["company"].fillna(""),
                "Email": sub["email"].fillna(""),
                "Status": sub["status"].fillna(""),
            }
        )
        return sub, display

    hot_raw, hot_df = build_group(df_all["status"].isin(["Quoted", "Meeting"]))
    pot_raw, pot_df = build_group(df_all["status"].isin(["New", "Contacted"]))
    cold_raw, cold_df = build_group(df_all["status"].isin(["Pending", "On hold", "Irrelevant"]))

    col1, col2, col3 = st.columns(3)

    link_col_config = {
        "Profile": st.column_config.LinkColumn("Profile", display_text="👤"),
        "Website": st.column_config.LinkColumn("Website", display_text="🌐"),
    }

    # HOT
    with col1:
        st.markdown(
            f"""
            <div style="background-color:#ff6b6b;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                🔥 Hot customers ({len(hot_df)}) — Quoted / Meeting
            </div>
            """,
            unsafe_allow_html=True,
        )
        if hot_df.empty:
            st.caption("No leads in this group.")
        else:
            st.dataframe(hot_df, hide_index=True, use_container_width=True, column_config=link_col_config)
            hot_options = {int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                           for row in hot_raw.itertuples()}
            selected_hot = st.selectbox("Pick hot lead to move", list(hot_options.keys()),
                                        format_func=lambda cid: hot_options.get(cid, str(cid)), key="hot_select")
            c_hot1, c_hot2 = st.columns(2)
            with c_hot1:
                if st.button("Move to Potential", key="btn_hot_to_pot"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_hot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    new_status = "Lost" if old == "Quoted" else ("Contacted" if old == "Meeting" else old)
                    update_contact_status(conn, selected_hot, new_status)
                    st.rerun()
            with c_hot2:
                if st.button("Move to Cold", key="btn_hot_to_cold"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_hot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    new_status = "Lost" if old == "Quoted" else ("Pending" if old == "Meeting" else old)
                    update_contact_status(conn, selected_hot, new_status)
                    st.rerun()

    # POTENTIAL
    with col2:
        st.markdown(
            f"""
            <div style="background-color:#28a745;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                🌱 Potential customers ({len(pot_df)}) — New / Contacted
            </div>
            """,
            unsafe_allow_html=True,
        )
        if pot_df.empty:
            st.caption("No leads in this group.")
        else:
            st.dataframe(pot_df, hide_index=True, use_container_width=True, column_config=link_col_config)
            pot_options = {int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                           for row in pot_raw.itertuples()}
            selected_pot = st.selectbox("Pick potential lead to move", list(pot_options.keys()),
                                        format_func=lambda cid: pot_options.get(cid, str(cid)), key="pot_select")
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

    # COLD
    with col3:
        st.markdown(
            f"""
            <div style="background-color:#007bff;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                ❄️ Cold customers ({len(cold_df)}) — Pending / On hold / Irrelevant
            </div>
            """,
            unsafe_allow_html=True,
        )
        if cold_df.empty:
            st.caption("No leads in this group.")
        else:
            st.dataframe(cold_df, hide_index=True, use_container_width=True, column_config=link_col_config)
            cold_options = {int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                           for row in cold_raw.itertuples()}
            selected_cold = st.selectbox("Pick cold lead to move", list(cold_options.keys()),
                                         format_func=lambda cid: cold_options.get(cid, str(cid)), key="cold_select")
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
# SIDEBAR IMPORT / EXPORT
# -------------------------------------------------------------
def sidebar_import_export(conn: sqlite3.Connection):
    st.sidebar.header("Import / Export")

    up = st.sidebar.file_uploader("Upload Excel/CSV (Contacts)", type=["xlsx", "xls", "csv"])
    if up is not None:
        df = load_contacts_file(up)
        n = upsert_contacts(conn, df)
        st.sidebar.success(f"Imported/updated {n} contacts")

    total = pd.read_sql_query("SELECT COUNT(*) n FROM contacts", conn).iloc[0]["n"]
    st.sidebar.caption(f"Total contacts: **{total}**")

    export_df = st.session_state.get("export_df")
    if isinstance(export_df, pd.DataFrame) and not export_df.empty:
        csv_bytes = export_df.to_csv(index=False).encode("utf-8")
        st.sidebar.download_button(
            "Download Contacts CSV (filtered)",
            csv_bytes,
            file_name="radom-contacts.csv",
        )


# -------------------------------------------------------------
# FILTERS UI
# -------------------------------------------------------------
def filters_ui():
    st.subheader("Filters")
    q = st.text_input("Search (name, email, company)", "")

    c1, c2, c3 = st.columns(3)
    with c1:
        cats = st.multiselect("Category",
                              ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"], [])
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
# CONTACT EDITOR + NOTES + WEBSITE LINK
# -------------------------------------------------------------
def contact_editor(conn: sqlite3.Connection, row: pd.Series):
    st.markdown("---")
    contact_id = int(row["id"])

    full_name = f"{row.get('first_name') or ''} {row.get('last_name') or ''}".strip()
    st.markdown(f"### ✏️ {full_name} — {row.get('company') or ''}")

    st.caption(
        f"Status: {row.get('status') or 'New'} | "
        f"Application: {row.get('application') or '—'} | "
        f"Owner: {row.get('owner') or '—'}"
    )

    profile_url_header = row.get("profile_url")
    if profile_url_header:
        u = _clean_url(profile_url_header)
        st.markdown(f"🔗 Profile: [{u}]({u})")

    website_header = row.get("website")
    if website_header:
        w = _clean_url(website_header)
        st.markdown(f"🌐 Website: [{w}]({w})")

    with st.form(f"edit_{contact_id}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            first = st.text_input("First name", row.get("first_name") or "")
            job = st.text_input("Job title", row.get("job_title") or "")
            phone = st.text_input("Phone", row.get("phone") or "")

            gender_options = ["", "Female", "Male", "Other"]
            raw_gender = row.get("gender") or ""
            current_gender = raw_gender if raw_gender in gender_options else ""
            gender = st.selectbox("Gender", gender_options, index=gender_options.index(current_gender))
        with col2:
            last = st.text_input("Last name", row.get("last_name") or "")
            company = st.text_input("Company", row.get("company") or "")
            email = st.text_input("Email", row.get("email") or "")
            app_options = [""] + APPLICATIONS
            current_app = row.get("application") or ""
            application = st.selectbox("Application", app_options,
                                       index=app_options.index(current_app) if current_app in app_options else 0)
        with col3:
            cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
            category = st.selectbox("Category", cat_opts,
                                    index=cat_opts.index(row.get("category")) if row.get("category") in cat_opts else 0)

            status = st.selectbox("Status", PIPELINE,
                                  index=PIPELINE.index(row.get("status")) if row.get("status") in PIPELINE else 0)

            product_options = [""] + PRODUCTS
            raw_prod = row.get("product_interest") or ""
            product = st.selectbox("Product type interest", product_options,
                                   index=product_options.index(raw_prod) if raw_prod in product_options else 0)

            owner_val = row.get("owner") or ""
            owner = st.selectbox("Owner", OWNER_OPTIONS,
                                 index=OWNER_OPTIONS.index(owner_val) if owner_val in OWNER_OPTIONS else 0)

        st.write("**Address**")
        street = st.text_input("Street", row.get("street") or "")
        street2 = st.text_input("Street 2", row.get("street2") or "")
        city = st.text_input("City", row.get("city") or "")
        state = st.text_input("State/Province", row.get("state") or "")
        zipc = st.text_input("ZIP", row.get("zip_code") or "")
        country = st.text_input("Country", row.get("country") or "")
        website = st.text_input("Website", row.get("website") or "")
        profile_url = st.text_input("Profile URL", row.get("profile_url") or "")

        col_save, col_delete = st.columns([3, 1])
        saved = col_save.form_submit_button("Save changes")
        delete_pressed = col_delete.form_submit_button("🗑️ Delete this contact")

        if saved:
            cur = conn.cursor()
            if (row.get("status") or "").strip() != (status or "").strip():
                cur.execute(
                    "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
                    (contact_id, datetime.utcnow().isoformat(), row.get("status"), status),
                )

            conn.execute(
                """
                UPDATE contacts SET
                    first_name=?, last_name=?, job_title=?, company=?, phone=?, email=?,
                    category=?, status=?, owner=?, street=?, street2=?, city=?, state=?,
                    zip_code=?, country=?, website=?, profile_url=?, last_touch=?, gender=?, application=?, product_interest=?
                WHERE id=?
                """,
                (
                    first or None,
                    last or None,
                    job or None,
                    company or None,
                    phone or None,
                    (email or "").lower().strip() or None,
                    category,
                    status,
                    owner or None,
                    street or None,
                    street2 or None,
                    city or None,
                    state or None,
                    zipc or None,
                    country or None,
                    website or None,
                    _clean_url(profile_url) if profile_url else None,
                    datetime.utcnow().isoformat(),
                    gender or None,
                    normalize_application(application),
                    product or None,
                    contact_id,
                ),
            )
            conn.commit()
            backup_contacts(conn)
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
                ts_iso = datetime.utcnow().isoformat()
                fu_iso = next_fu.isoformat() if isinstance(next_fu, date) else None
                conn.execute(
                    "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                    (contact_id, ts_iso, new_note.strip(), fu_iso),
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


# -------------------------------------------------------------
# MANUAL ADD CONTACT FORM (with OWNER dropdown + NOTES field)
# -------------------------------------------------------------
def _clear_add_form():
    keys = [
        "add_first","add_job","add_phone","add_gender","add_last","add_company","add_email",
        "add_application","add_category","add_status","add_owner","add_product",
        "add_street","add_street2","add_city","add_state","add_zip","add_country",
        "add_website","add_profile_url","add_note","add_nextfu"
    ]
    for k in keys:
        st.session_state.pop(k, None)


def add_contact_form(conn: sqlite3.Connection):
    st.markdown("### ➕ Add new contact manually")

    with st.expander("Open form"):
        with st.form("add_contact_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                first = st.text_input("First name", key="add_first")
                job = st.text_input("Job title", key="add_job")
                phone = st.text_input("Phone", key="add_phone")
                gender = st.selectbox("Gender", ["", "Female", "Male", "Other"], key="add_gender")
            with col2:
                last = st.text_input("Last name", key="add_last")
                company = st.text_input("Company", key="add_company")
                email = st.text_input("Email", key="add_email")
                application_raw = st.selectbox("Application", [""] + APPLICATIONS, key="add_application")
            with col3:
                cat_opts = ["PhD/Student","Professor/Academic","Academic","Industry","Other"]
                category = st.selectbox("Category", cat_opts, index=3, key="add_category")
                status = st.selectbox("Status", PIPELINE, index=0, key="add_status")
                owner = st.selectbox("Owner", OWNER_OPTIONS, key="add_owner")
                product = st.selectbox("Product type interest", [""] + PRODUCTS, key="add_product")

            st.write("**Address**")
            street = st.text_input("Street", key="add_street")
            street2 = st.text_input("Street 2", key="add_street2")
            city = st.text_input("City", key="add_city")
            state = st.text_input("State/Province", key="add_state")
            zipc = st.text_input("ZIP", key="add_zip")
            country = st.text_input("Country", key="add_country")
            website = st.text_input("Website", key="add_website")
            profile_url = st.text_input("Profile URL", key="add_profile_url")

            st.write("**Optional note (saved immediately)**")
            note = st.text_area("Note", key="add_note", placeholder="Met at conference; interested in nitrification…")
            next_fu = st.date_input("Next follow-up", value=date.today(), key="add_nextfu")

            submitted = st.form_submit_button("Create contact")

        # Clear button OUTSIDE the form -> works reliably
        if st.button("Clear form", key="btn_clear_add_form"):
            _clear_add_form()
            st.rerun()

        if submitted:
            if not st.session_state.get("add_email") and not (
                st.session_state.get("add_first") and st.session_state.get("add_last") and st.session_state.get("add_company")
            ):
                st.error("Please provide either an email, or first name + last name + company.")
                return

            scan_dt = datetime.utcnow().isoformat()
            email_norm = (st.session_state.get("add_email") or "").strip().lower() or None
            status_norm = normalize_status(st.session_state.get("add_status")) or "New"
            application_norm = normalize_application(st.session_state.get("add_application"))
            website_norm = _clean_url(st.session_state.get("add_website")) if st.session_state.get("add_website") else None
            profile_norm = _clean_url(st.session_state.get("add_profile_url")) if st.session_state.get("add_profile_url") else None

            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO contacts
                (scan_datetime, first_name, last_name, job_title, company,
                 street, street2, zip_code, city, state, country,
                 phone, email, website, profile_url, category, status, owner, last_touch,
                 gender, application, product_interest)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    scan_dt,
                    st.session_state.get("add_first") or None,
                    st.session_state.get("add_last") or None,
                    st.session_state.get("add_job") or None,
                    st.session_state.get("add_company") or None,
                    st.session_state.get("add_street") or None,
                    st.session_state.get("add_street2") or None,
                    st.session_state.get("add_zip") or None,
                    st.session_state.get("add_city") or None,
                    st.session_state.get("add_state") or None,
                    st.session_state.get("add_country") or None,
                    st.session_state.get("add_phone") or None,
                    email_norm,
                    website_norm,
                    profile_norm,
                    st.session_state.get("add_category"),
                    status_norm,
                    st.session_state.get("add_owner") or None,
                    scan_dt,
                    st.session_state.get("add_gender") or None,
                    application_norm,
                    st.session_state.get("add_product") or None,
                ),
            )
            contact_id = cur.lastrowid

            # Save note if provided
            note_text = (st.session_state.get("add_note") or "").strip()
            if note_text:
                fu = st.session_state.get("add_nextfu")
                fu_iso = fu.isoformat() if isinstance(fu, date) else None
                conn.execute(
                    "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                    (contact_id, scan_dt, note_text, fu_iso),
                )

            conn.commit()
            backup_contacts(conn)
            st.success("New contact created")
            _clear_add_form()
            st.rerun()


# -------------------------------------------------------------
# MAIN APP
# -------------------------------------------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    check_login_two_factor_telegram()

    conn = get_conn()
    init_db(conn)
    restore_from_backup_if_empty(conn)

    top_l, top_r = st.columns([3, 1])
    with top_l:
        st.title(APP_TITLE)
        st.caption("Upload leads → categorize → work the pipeline → export.")
    with top_r:
        show_won_counter(conn)

    sidebar_import_export(conn)
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
        st.info(
            "No contacts match your filters or the database is empty. "
            "Add a contact with the form above or upload an Excel/CSV in the sidebar."
        )
        return

    export_cols = [
        "first_name","last_name","email","phone","job_title","company","city","state","country",
        "category","status","owner","gender","application","product_interest","last_touch",
        "notes","website","profile_url",
    ]
    available_cols = [c for c in export_cols if c in df.columns]
    st.session_state["export_df"] = df[available_cols].copy()

    # Display table with clickable links
    display_df = df[available_cols].copy()
    if "profile_url" in display_df.columns:
        display_df["profile_url"] = display_df["profile_url"].fillna("").apply(_clean_url)
    if "website" in display_df.columns:
        display_df["website"] = display_df["website"].fillna("").apply(_clean_url)

    col_config = {}
    if "profile_url" in display_df.columns:
        col_config["profile_url"] = st.column_config.LinkColumn("Profile", display_text="👤")
    if "website" in display_df.columns:
        col_config["website"] = st.column_config.LinkColumn("Website", display_text="🌐")

    st.subheader("Contacts")
    st.dataframe(display_df, use_container_width=True, hide_index=True, column_config=col_config)

    options = [
        (int(r.id), f"{(r.first_name or '')} {(r.last_name or '')}".strip() + f" — {r.company or ''}")
        for r in df[["id", "first_name", "last_name", "company"]].itertuples(index=False)
    ]
    if not options:
        return

    chosen_id = st.selectbox(
        "Select a contact to edit",
        [opt[0] for opt in options],
        format_func=lambda x: dict(options).get(x, str(x)),
    )

    if chosen_id:
        row = df[df["id"] == chosen_id].iloc[0]
        contact_editor(conn, row)


if __name__ == "__main__":
    main()
