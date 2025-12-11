import os
import re
import sqlite3
import random
import time
from datetime import datetime, date
from typing import List, Any, Optional, Tuple, Dict

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

OWNER_CHOICES = ["", "Velibor", "Liz", "Jovan", "Ian", "Qi", "Kenshin"]

# -------------------------------------------------------------
# TELEGRAM OTP (2-FACTOR) — username first, then password
# -------------------------------------------------------------
def _tg_token() -> Optional[str]:
    return st.secrets.get("TELEGRAM_BOT_TOKEN")

def _tg_admin_username() -> str:
    return (st.secrets.get("ADMIN_USERNAME") or "").strip().lstrip("@")

def _tg_admin_chat_id() -> Optional[str]:
    v = st.secrets.get("ADMIN_CHAT_ID")
    return str(v).strip() if v is not None else None

def _send_telegram_message(chat_id: str, text: str) -> Tuple[bool, int, str]:
    """Send a Telegram message. Returns (ok, status_code, response_text)."""
    token = _tg_token()
    if not token:
        return False, 0, "Missing TELEGRAM_BOT_TOKEN in secrets"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        return (resp.status_code == 200), resp.status_code, resp.text
    except Exception as e:
        return False, 0, f"Exception: {e}"

def _telegram_get_updates() -> Tuple[bool, str, Optional[dict]]:
    """Fetch bot updates (for chat-id auto detection)."""
    token = _tg_token()
    if not token:
        return False, "Missing TELEGRAM_BOT_TOKEN in secrets", None
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}: {resp.text}", None
        data = resp.json()
        return True, "ok", data
    except Exception as e:
        return False, f"Exception: {e}", None

def _detect_chat_id_for_username(username_no_at: str) -> Optional[str]:
    """
    Best-effort detection using getUpdates.
    Works only if user has messaged the bot at least once (e.g., /start, hi).
    """
    u = (username_no_at or "").strip().lstrip("@").lower()
    if not u:
        return None

    ok, msg, data = _telegram_get_updates()
    if not ok or not data:
        return None

    results = data.get("result", [])
    # Look from newest to oldest
    for upd in reversed(results):
        # message
        m = upd.get("message") or upd.get("edited_message") or None
        if not m:
            continue
        chat = m.get("chat") or {}
        frm = m.get("from") or {}
        from_user = (frm.get("username") or "").lower()
        if from_user == u:
            cid = chat.get("id")
            if cid is not None:
                return str(cid)
    return None

def _get_target_chat_id(username_no_at: str) -> Tuple[Optional[str], str]:
    """
    Resolve chat_id for OTP delivery.
    Priority:
      1) If username matches ADMIN_USERNAME and ADMIN_CHAT_ID exists -> use ADMIN_CHAT_ID (most reliable)
      2) Try detect via getUpdates
    """
    u = (username_no_at or "").strip().lstrip("@")
    if not u:
        return None, "Missing Telegram username"

    if u.lower() == _tg_admin_username().lower():
        admin_id = _tg_admin_chat_id()
        if admin_id:
            return admin_id, "admin_chat_id"
        # fall through to detection if admin chat id missing

    cid = _detect_chat_id_for_username(u)
    if cid:
        return cid, "detected"
    return None, "not_found"

def check_login_two_factor_telegram():
    expected_pw = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)

    ss = st.session_state
    ss.setdefault("authed", False)
    ss.setdefault("auth_stage", "login")  # login -> otp

    if ss["authed"]:
        return

    st.sidebar.header("🔐 Login")

    # --- Login inputs (username first, then password) ---
    username = st.sidebar.text_input("Telegram username (without @)", key="login_username_input")
    pwd = st.sidebar.text_input("Password", type="password", key="login_password_input")

    # --- Test Telegram button ---
    st.sidebar.caption("Troubleshooting")
    if st.sidebar.button("🧪 Test Telegram", key="btn_test_telegram"):
        # Try to target admin chat id first if available
        target_cid = _tg_admin_chat_id()
        if not target_cid:
            # If admin chat id missing, try detect from username
            target_cid, how = _get_target_chat_id(username)
        else:
            how = "admin_chat_id"

        if not target_cid:
            st.sidebar.error("No chat_id available yet. Make sure you started the bot chat and set ADMIN_CHAT_ID.")
        else:
            ok, code, txt = _send_telegram_message(target_cid, f"✅ Telegram test from {APP_TITLE} ({how})")
            st.sidebar.write(f"Status: {code}")
            st.sidebar.write(txt)
            if ok:
                st.sidebar.success("Test message sent. Check your Telegram chat with the bot.")

    st.sidebar.markdown("---")

    # --- Continue button generates OTP and tries to send ---
    if st.sidebar.button("Continue", key="btn_login_continue"):
        if (pwd or "") != expected_pw:
            st.sidebar.error("Wrong password")
            st.stop()

        u = (username or "").strip().lstrip("@")
        if not u:
            st.sidebar.error("Please enter your Telegram username (without @).")
            st.stop()

        chat_id, how = _get_target_chat_id(u)

        # Generate OTP
        code = f"{random.randint(0, 999999):06d}"
        ss["otp_code"] = code
        ss["otp_time"] = int(time.time())
        ss["login_username"] = u
        ss["auth_stage"] = "otp"

        # Try send to Telegram if we have chat_id
        if chat_id:
            ok, status, resp_text = _send_telegram_message(
                chat_id,
                f"🔐 {APP_TITLE} login code for @{u}: {code} (valid {OTP_TTL_SECONDS//60} min)"
            )
            if ok:
                st.sidebar.success("✅ Code sent to Telegram.")
            else:
                st.sidebar.error("Failed to send Telegram message.")
                st.sidebar.write(f"Telegram response: {status} — {resp_text}")
                st.sidebar.info(f"Use this one-time code instead: **{code}**")
        else:
            # No chat id; show fallback code in sidebar
            st.sidebar.error(
                "Could not detect your Telegram chat. Open Telegram, search for the bot, press Start, send 'hi', then try again."
            )
            st.sidebar.info(f"For now, use this one-time code: **{code}**")

        st.rerun()

    # --- OTP stage ---
    if ss.get("auth_stage") != "otp":
        st.stop()

    # Expiration check
    if "otp_time" in ss and int(time.time()) - ss["otp_time"] > OTP_TTL_SECONDS:
        for k in ("otp_code", "otp_time", "login_username"):
            ss.pop(k, None)
        ss["auth_stage"] = "login"
        st.sidebar.error("Code expired. Please start over.")
        st.stop()

    st.sidebar.write("Enter the 6-digit code sent to Telegram, or use the backup code shown below.")

    # Always show backup OTP so it never feels “lost”
    current_code = ss.get("otp_code")
    if current_code:
        st.sidebar.info(f"Backup code (current session): **{current_code}**")

    code_in = st.sidebar.text_input("Enter 6-digit code", max_chars=6, key="login_otp_input")
    if st.sidebar.button("Verify", key="btn_verify_otp"):
        if code_in.strip() == ss.get("otp_code", ""):
            ss["authed"] = True
            ss["auth_stage"] = "login"
            for k in ("otp_code", "otp_time"):
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

    # Ensure columns exist for older DBs
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(contacts)")
    cols = [row[1] for row in cur.fetchall()]
    if "profile_url" not in cols:
        cur.execute("ALTER TABLE contacts ADD COLUMN profile_url TEXT")
    if "photo" not in cols:
        cur.execute("ALTER TABLE contacts ADD COLUMN photo TEXT")
    if "owner" not in cols:
        cur.execute("ALTER TABLE contacts ADD COLUMN owner TEXT")

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
# FLEXIBLE FILE LOADER (handles Book2.xlsx-style files)
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

    known = set(COLMAP.keys()) | set(EXPECTED) | {
        "first_name", "last_name", "email", "phone", "job_title", "company",
        "city", "state", "country", "category", "status", "owner",
        "gender", "application", "product_interest", "last_touch",
        "notes", "photo", "profile_url"
    }

    score = sum(1 for v in first_vals_lower if v in known)
    if score >= 3:
        new_cols = []
        for i, val in enumerate(first_vals_lower):
            if val == "":
                new_cols.append(f"extra_{i}")
            else:
                new_cols.append(val)
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
        email = r["email"].strip().lower() or None
        status_from_file = r.get("status_norm")
        note_text = (r.get("notes") or "").strip()
        photo_path = (r.get("photo") or "").strip() or None
        profile_url = (r.get("profile_url") or "").strip() or None
        owner = (r.get("owner") or "").strip() or None

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
                str(r["phone"]) if r["phone"] != "" else None,
                email,
                r["website"],
                r["category"],
            )

            gender = r.get("gender") or None
            application = normalize_application(r.get("application"))
            product_interest = r.get("product_interest") or None

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
                        category=?, status=?, owner=?, gender=?, application=?, product_interest=?, photo=?, profile_url=?
                    WHERE id=?
                    """,
                    payload_common
                    + (
                        final_status,
                        owner,
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
                     city, state, country, phone, email, website, category, status, owner, gender, application,
                     product_interest, photo, profile_url)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    payload_common
                    + (
                        final_status,
                        owner,
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
# TOP PRIORITY LISTS (HOT / POTENTIAL / COLD)
# -------------------------------------------------------------
def show_priority_lists(conn: sqlite3.Connection):
    st.subheader("Customer overview")

    df_all = pd.read_sql_query(
        "SELECT id, first_name, last_name, company, email, status, profile_url FROM contacts",
        conn,
    )

    if df_all.empty:
        st.caption("No contacts yet – add someone manually or import a file.")
        return

    df_all["status"] = df_all["status"].fillna("New").astype(str).str.strip()

    def build_group(mask):
        sub = df_all[mask].copy()
        cols = ["Profile", "Name", "Company", "Email", "Status"]
        if sub.empty:
            return sub, pd.DataFrame(columns=cols)

        sub["Name"] = (sub["first_name"].fillna("") + " " + sub["last_name"].fillna("")).str.strip()
        sub["Profile"] = sub.get("profile_url", "").fillna("")
        display = sub[["Profile", "Name", "company", "email", "status"]].rename(
            columns={"company": "Company", "email": "Email", "status": "Status"}
        )
        return sub, display

    hot_raw, hot_df = build_group(df_all["status"].isin(["Quoted", "Meeting"]))
    pot_raw, pot_df = build_group(df_all["status"].isin(["New", "Contacted"]))
    cold_raw, cold_df = build_group(df_all["status"].isin(["Pending", "On hold", "Irrelevant"]))

    col1, col2, col3 = st.columns(3)
    link_col_config = {"Profile": st.column_config.LinkColumn("Profile", display_text="👤")}

    # HOT PANEL
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

            hot_options = {
                int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                for row in hot_raw.itertuples()
            }
            selected_hot = st.selectbox("Pick hot lead to move", list(hot_options.keys()),
                                        format_func=lambda cid: hot_options.get(cid, str(cid)),
                                        key="hot_select")
            c_hot1, c_hot2 = st.columns(2)
            with c_hot1:
                if st.button("Move to Potential", key="btn_hot_to_pot"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_hot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    if old == "Quoted":
                        new_status = "Lost"
                    elif old == "Meeting":
                        new_status = "Contacted"
                    else:
                        new_status = old
                    update_contact_status(conn, selected_hot, new_status)
                    st.rerun()
            with c_hot2:
                if st.button("Move to Cold", key="btn_hot_to_cold"):
                    cur = conn.cursor()
                    cur.execute("SELECT status FROM contacts WHERE id=?", (selected_hot,))
                    old = (cur.fetchone() or ["New"])[0]
                    old = (old or "New").strip()
                    if old == "Quoted":
                        new_status = "Lost"
                    elif old == "Meeting":
                        new_status = "Pending"
                    else:
                        new_status = old
                    update_contact_status(conn, selected_hot, new_status)
                    st.rerun()

    # POTENTIAL PANEL
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

            pot_options = {
                int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                for row in pot_raw.itertuples()
            }
            selected_pot = st.selectbox("Pick potential lead to move", list(pot_options.keys()),
                                        format_func=lambda cid: pot_options.get(cid, str(cid)),
                                        key="pot_select")
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
                    if old == "New":
                        new_status = "Irrelevant"
                    elif old == "Contacted":
                        new_status = "Pending"
                    else:
                        new_status = old
                    update_contact_status(conn, selected_pot, new_status)
                    st.rerun()

    # COLD PANEL
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

            cold_options = {
                int(row.id): f"{row.first_name} {row.last_name} — {row.company or ''} ({row.email or ''}) [{row.status}]"
                for row in cold_raw.itertuples()
            }
            selected_cold = st.selectbox("Pick cold lead to move", list(cold_options.keys()),
                                         format_func=lambda cid: cold_options.get(cid, str(cid)),
                                         key="cold_select")
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
        cats = st.multiselect(
            "Category",
            ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"],
            [],
        )
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
# CONTACT EDITOR + NOTES
# -------------------------------------------------------------
def contact_editor(conn: sqlite3.Connection, row: pd.Series):
    st.markdown("---")

    contact_id = int(row["id"])

    st.markdown(f"### ✏️ {row['first_name']} {row['last_name']} — {row.get('company') or ''}")
    st.caption(f"Status: {row.get('status') or 'New'} | Application: {row.get('application') or '—'}")

    profile_url_header = row.get("profile_url")
    if profile_url_header:
        st.markdown(f"🔗 Profile: [{profile_url_header}]({profile_url_header})")

    website_header = row.get("website")
    if website_header:
        w = str(website_header).strip()
        if w and not w.lower().startswith(("http://", "https://")):
            w = "https://" + w
        st.markdown(f"🌐 Website: [{w}]({w})")

    with st.form(f"edit_{contact_id}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            first = st.text_input("First name", row["first_name"] or "")
            job = st.text_input("Job title", row["job_title"] or "")
            phone = st.text_input("Phone", row["phone"] or "")

            gender_options = ["", "Female", "Male", "Other"]
            raw_gender = row.get("gender") or ""
            current_gender = raw_gender if raw_gender in gender_options else ""
            gender = st.selectbox("Gender", gender_options, index=gender_options.index(current_gender))

        with col2:
            last = st.text_input("Last name", row["last_name"] or "")
            company = st.text_input("Company", row["company"] or "")
            email = st.text_input("Email", row["email"] or "")

            app_options = [""] + APPLICATIONS
            current_app = row.get("application") or ""
            app_index = app_options.index(current_app) if current_app in app_options else 0
            application = st.selectbox("Application", app_options, index=app_index)

        with col3:
            cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
            category = st.selectbox("Category", cat_opts,
                                    index=cat_opts.index(row["category"]) if row["category"] in cat_opts else 0)

            status = st.selectbox("Status", PIPELINE,
                                  index=PIPELINE.index(row["status"]) if row["status"] in PIPELINE else 0)

            product_options = [""] + PRODUCTS
            raw_prod = row.get("product_interest") or ""
            prod_index = product_options.index(raw_prod) if raw_prod in product_options else 0
            product = st.selectbox("Product type interest", product_options, index=prod_index)

            # Owner dropdown
            current_owner = (row.get("owner") or "").strip()
            owner_index = OWNER_CHOICES.index(current_owner) if current_owner in OWNER_CHOICES else 0
            owner = st.selectbox("Owner", OWNER_CHOICES, index=owner_index)

        st.write("**Address**")
        street = st.text_input("Street", row["street"] or "")
        street2 = st.text_input("Street 2", row["street2"] or "")
        city = st.text_input("City", row["city"] or "")
        state = st.text_input("State/Province", row["state"] or "")
        zipc = st.text_input("ZIP", row["zip_code"] or "")
        country = st.text_input("Country", row["country"] or "")
        website = st.text_input("Website", row.get("website") or "")
        profile_url = st.text_input("Profile URL", row.get("profile_url") or "")

        col_save, col_delete = st.columns([3, 1])
        with col_save:
            saved = st.form_submit_button("Save changes")
        with col_delete:
            delete_pressed = st.form_submit_button("🗑️ Delete this contact")

        if saved:
            cur = conn.cursor()
            if (row["status"] or "").strip() != (status or "").strip():
                cur.execute(
                    "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
                    (contact_id, datetime.utcnow().isoformat(), row["status"], status),
                )

            email_norm = (email or "").lower().strip() or None
            website_norm = (website or "").strip() or None
            profile_norm = (profile_url or "").strip() or None

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
                    email_norm,
                    category,
                    status,
                    owner or None,
                    street or None,
                    street2 or None,
                    city or None,
                    state or None,
                    zipc or None,
                    country or None,
                    website_norm,
                    profile_norm,
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
# MANUAL ADD CONTACT FORM (with Owner dropdown)
# -------------------------------------------------------------
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
                cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
                category = st.selectbox("Category", cat_opts, index=3, key="add_category")
                status = st.selectbox("Status", PIPELINE, index=0, key="add_status")

                # Owner dropdown
                owner = st.selectbox("Owner", OWNER_CHOICES, index=0, key="add_owner")

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

            col_create, col_clear = st.columns([3, 1])
            with col_create:
                submitted = st.form_submit_button("Create contact")
            with col_clear:
                clear = st.form_submit_button("Clear form")

            if submitted:
                if not email and not (first and last and company):
                    st.error("Please provide either an email, or first name + last name + company.")
                else:
                    scan_dt = datetime.utcnow().isoformat()
                    email_norm = (email or "").strip().lower() or None
                    status_norm = normalize_status(status) or "New"
                    application_norm = normalize_application(application_raw)

                    conn.execute(
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
                            first or None,
                            last or None,
                            job or None,
                            company or None,
                            street or None,
                            street2 or None,
                            zipc or None,
                            city or None,
                            state or None,
                            country or None,
                            phone or None,
                            email_norm,
                            (website or "").strip() or None,
                            (profile_url or "").strip() or None,
                            category,
                            status_norm,
                            owner or None,
                            scan_dt,
                            gender or None,
                            application_norm,
                            product or None,
                        ),
                    )
                    conn.commit()
                    backup_contacts(conn)
                    st.success("New contact created")
                    st.rerun()

            if clear:
                for key in [
                    "add_first", "add_job", "add_phone", "add_gender",
                    "add_last", "add_company", "add_email", "add_application",
                    "add_category", "add_status", "add_owner", "add_product",
                    "add_street", "add_street2", "add_city", "add_state",
                    "add_zip", "add_country", "add_website", "add_profile_url",
                ]:
                    st.session_state.pop(key, None)
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
        "first_name", "last_name", "email", "phone", "job_title", "company",
        "city", "state", "country", "category", "status", "owner", "gender",
        "application", "product_interest", "last_touch", "notes", "website", "profile_url",
    ]
    available_cols = [c for c in export_cols if c in df.columns]

    st.session_state["export_df"] = df[available_cols].copy()

    display_df = df[available_cols].copy()

    # Make website + profile look like link columns in UI
    col_config = {}
    if "profile_url" in display_df.columns:
        display_df = display_df.rename(columns={"profile_url": "Profile URL"})
        col_config["Profile URL"] = st.column_config.LinkColumn("Profile URL", display_text="👤")

    if "website" in display_df.columns:
        col_config["website"] = st.column_config.LinkColumn("website", display_text="🌐")

        # Normalize website links visually (add https:// if missing)
        def _norm_web(v):
            if v is None:
                return ""
            s = str(v).strip()
            if not s:
                return ""
            if not s.lower().startswith(("http://", "https://")):
                return "https://" + s
            return s
        display_df["website"] = display_df["website"].apply(_norm_web)

    st.subheader("Contacts")
    st.dataframe(display_df, use_container_width=True, hide_index=True, column_config=col_config)

    options = [
        (int(r.id), f"{r.first_name} {r.last_name} — {r.company or ''}")
        for r in df[["id", "first_name", "last_name", "company"]].itertuples(index=False)
    ]
    if not options:
        return

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
