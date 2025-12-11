import os
import re
import sqlite3
import random
import time
from datetime import datetime, date
from typing import List, Any, Optional

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

# Fixed list of owners
OWNER_CHOICES = ["", "Velibor", "Liz", "Jovan", "Ian", "Qi", "Kenshin"]

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

        -- Per-user Telegram mapping for multi-user 2FA
        CREATE TABLE IF NOT EXISTS users (
          username TEXT PRIMARY KEY,
          chat_id TEXT
        );
        """
    )

    # Make sure new columns exist for older DBs
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
# TELEGRAM MULTI-USER 2FA HELPERS
# -------------------------------------------------------------
def get_or_detect_chat_id(
    conn: sqlite3.Connection, username: str, token: str
) -> Optional[str]:
    """
    For a given Telegram username, resolve the chat_id:

    1) Look in local 'users' table.
    2) If not found, call getUpdates on the bot and search for that username.
    3) If still not found and this is the admin user, fall back to ADMIN_CHAT_ID secret.
    4) Cache found chat_id in 'users' for future logins.
    """
    username_clean = (username or "").strip().lstrip("@").lower()
    if not username_clean:
        return None

    admin_username = (st.secrets.get("ADMIN_USERNAME", "") or "").strip().lstrip("@").lower()
    admin_chat_id = st.secrets.get("ADMIN_CHAT_ID")

    cur = conn.cursor()
    cur.execute(
        "SELECT chat_id FROM users WHERE LOWER(username)=?",
        (username_clean,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0])

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception:
        data = None

    found_chat_id = None

    if data and data.get("ok"):
        for upd in data.get("result", []):
            msg = (
                upd.get("message")
                or upd.get("my_chat_member")
                or upd.get("edited_message")
                or upd.get("channel_post")
            )
            if not msg:
                continue
            user_obj = msg.get("from") or {}
            chat_obj = msg.get("chat") or {}
            uname = str(user_obj.get("username", "")).lower()
            if uname == username_clean:
                found_chat_id = chat_obj.get("id")
                if found_chat_id:
                    break

    # ✅ Fallback: if this is the admin user and we have ADMIN_CHAT_ID, use it
    if not found_chat_id and username_clean == admin_username and admin_chat_id:
        found_chat_id = str(admin_chat_id)

    if found_chat_id:
        try:
            cur.execute(
                "INSERT OR REPLACE INTO users(username, chat_id) VALUES(?,?)",
                (username_clean, str(found_chat_id)),
            )
            conn.commit()
        except sqlite3.Error:
            pass
        return str(found_chat_id)

    return None

def send_otp_via_telegram(token: str, chat_id: str, code: str, username: str) -> bool:
    """Send the OTP to the specific user's chat_id."""
    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = f"Radom CRM login code for @{username}: {code} (valid {OTP_TTL_SECONDS//60} min)"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


def check_login_two_factor_multiuser(conn: sqlite3.Connection):
    """
    Multi-user login:

    1) Everyone shares the same app password (APP_PASSWORD / DEFAULT_PASSWORD).
    2) Then each user enters *their* Telegram username.
    3) App auto-detects chat_id for that username via bot getUpdates, sends OTP.
    4) Fallback: on-screen code if Telegram fails or user prefers.
    5) Admin (ADMIN_USERNAME) can reset the users->chat_id mapping.
    """
    expected = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)
    admin_username = st.secrets.get("ADMIN_USERNAME", "liza")

    ss = st.session_state
    ss.setdefault("auth_pw_ok", False)
    ss.setdefault("authed", False)
    ss.setdefault("username", "")

    st.sidebar.header("🔐 Login")

    # Already authenticated
    if ss["authed"]:
        user = ss.get("username") or "user"
        st.sidebar.success(f"Logged in as {user}")

        if st.sidebar.button("Log out"):
            for k in ("auth_pw_ok", "authed", "username", "otp_code", "otp_time", "chat_id"):
                ss.pop(k, None)
            st.rerun()

        # Admin-only tools
        if user and user.lower() == admin_username.lower():
            with st.sidebar.expander("Admin tools"):
                if st.button("Reset Telegram mappings"):
                    conn.execute("DELETE FROM users")
                    conn.commit()
                    st.success("Telegram user mapping table cleared.")
        return

    # STEP 1 — Password
    if not ss["auth_pw_ok"]:
        pwd = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Next"):
            if pwd == expected:
                ss["auth_pw_ok"] = True
                st.rerun()
            else:
                st.sidebar.error("Wrong password")
        st.stop()

    # Expired OTP?
    if ss.get("otp_time") and int(time.time()) - ss["otp_time"] > OTP_TTL_SECONDS:
        for k in ("otp_code", "otp_time"):
            ss.pop(k, None)
        st.sidebar.error("Code expired. Please request a new one.")

    # STEP 2 — Username + send OTP
    username_input = st.sidebar.text_input(
        "Telegram username (without @)",
        value=ss.get("username", ""),
        help="Each user should type their own Telegram username. Make sure you've started the bot in Telegram first.",
    )

    col_u1, col_u2 = st.sidebar.columns(2)
    with col_u1:
        send_pressed = st.button("Send code")
    with col_u2:
        local_pressed = st.button("Use on-screen code")

    token = st.secrets.get("TELEGRAM_BOT_TOKEN")

    # Request a new code (either via Telegram or locally)
    if send_pressed or local_pressed:
        code = f"{random.randint(0, 999999):06d}"
        ss["otp_code"] = code
        ss["otp_time"] = int(time.time())

        # Local-only fallback (no Telegram needed)
        if local_pressed:
            ss["username"] = username_input.strip() or "local-user"
            st.sidebar.info(f"Your one-time code: **{code}**")
            st.stop()

        username_clean = username_input.strip().lstrip("@")
        if not username_clean:
            st.sidebar.error("Please enter your Telegram username before sending a code.")
            st.stop()

        ss["username"] = username_clean

        if not token:
            st.sidebar.warning("Telegram bot token is not configured.")
            st.sidebar.info(f"Use this one-time code instead: **{code}**")
            st.stop()

        chat_id = get_or_detect_chat_id(conn, username_clean, token)
        if not chat_id:
            st.sidebar.warning(
                "Could not detect your Telegram chat. "
                "Open Telegram, search for the bot, press Start, then try again."
            )
            st.sidebar.info(f"For now, use this one-time code: **{code}**")
            st.stop()

        ss["chat_id"] = chat_id
        ok = send_otp_via_telegram(token, chat_id, code, username_clean)
        if ok:
            st.sidebar.success("Code sent to your Telegram.")
            st.sidebar.caption(
                "If Telegram is slow, you can still use the on-screen code as a backup."
            )
        else:
            st.sidebar.error("Failed to send Telegram message.")
            st.sidebar.info(f"Use this one-time code instead: **{code}**")
        st.stop()

    # STEP 3 — Verify OTP
    if ss.get("otp_code"):
        code_in = st.sidebar.text_input("Enter 6-digit code", max_chars=6)
        if st.sidebar.button("Verify code"):
            if code_in.strip() == ss["otp_code"]:
                ss["authed"] = True
                for k in ("otp_code", "otp_time"):
                    ss.pop(k, None)
                st.rerun()
            else:
                st.sidebar.error("Incorrect code")
        st.stop()
    else:
        # No OTP yet; stop app until user requests one
        st.stop()


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
    # various ways of naming profile links
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
    "photo",
    "owner",
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

    # exact match to list first
    for app in APPLICATIONS:
        if s == app.lower():
            return app

    # keyword-based mapping
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

    # If we already have 'first_name' or 'first name' as a column, nothing to do.
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
        "last_touch",
        "notes",
        "photo",
        "profile_url",
    }

    score = sum(1 for v in first_vals_lower if v in known)

    # Heuristic: if we see at least 3 known header names in row 0, treat it as header.
    if score >= 3:
        new_cols = []
        for i, val in enumerate(first_vals_lower):
            if val == "":
                new_cols.append(f"extra_{i}")
            else:
                new_cols.append(val)
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_cols

        # Drop completely empty 'extra_*' columns
        for c in list(df.columns):
            if c.startswith("extra_") and df[c].isna().all():
                df = df.drop(columns=[c])

    return df


def load_contacts_file(uploaded_file) -> pd.DataFrame:
    """Load CSV/XLSX and fix header row if needed."""
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
        owner_val = (r.get("owner") or "").strip() or None

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
                        owner_val,
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
                        owner_val,
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
        sql += (
            " AND product_interest IN (" + ",".join("?" for _ in prod_filter) + ")"
        )
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
        .apply(
            lambda s: " || ".join(
                [str(x).strip() for x in s if str(x).strip() != ""]
            )
        )
        .reset_index(name="notes")
    )
    return grouped


def update_contact_status(conn: sqlite3.Connection, contact_id: int, new_status: str):
    """Update status + history; strip spaces to avoid 'New ' vs 'New' issues."""
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
# COUNTRY FLAG HELPER
# -------------------------------------------------------------
def country_to_flag(country: Optional[str]) -> str:
    """
    Convert a country string to an emoji flag.
    Very simple: expects 2-letter code (US, DE, etc.).
    Also handles a few common cases like 'USA' -> 'US'.
    """
    if not country:
        return ""
    s = country.strip().upper()
    if s == "USA" or s == "U.S.A.":
        s = "US"
    if len(s) != 2 or not s.isalpha():
        return ""
    base = ord("🇦")  # regional indicator A
    return chr(base + ord(s[0]) - ord("A")) + chr(base + ord(s[1]) - ord("A"))


# -------------------------------------------------------------
# WON COUNTER (simple + companies below)
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
    companies = (
        df_companies["company"].dropna().tolist() if not df_companies.empty else []
    )

    # Card with number only
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

    # Companies list just under the card
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
        "SELECT id, first_name, last_name, company, email, status, profile_url, country FROM contacts",
        conn,
    )

    if df_all.empty:
        st.caption("No contacts yet – add someone manually or import a file.")
        return

    # Make sure status has no stray spaces
    df_all["status"] = df_all["status"].fillna("New").astype(str).str.strip()

    # Add flag column
    df_all["flag"] = df_all["country"].apply(country_to_flag)

    def build_group(mask):
        sub = df_all[mask].copy()
        cols = ["Flag", "Profile", "Name", "Company", "Email", "Status"]
        if sub.empty:
            return sub, pd.DataFrame(columns=cols)

        sub["Name"] = (
            sub["first_name"].fillna("") + " " + sub["last_name"].fillna("")
        ).str.strip()
        sub["Profile"] = sub.get("profile_url", "").fillna("")
        display = sub[["flag", "Profile", "Name", "company", "email", "status"]].rename(
            columns={
                "flag": "Flag",
                "company": "Company",
                "email": "Email",
                "status": "Status",
            }
        )
        return sub, display

    hot_raw, hot_df = build_group(df_all["status"].isin(["Quoted", "Meeting"]))
    pot_raw, pot_df = build_group(df_all["status"].isin(["New", "Contacted"]))
    cold_raw, cold_df = build_group(
        df_all["status"].isin(["Pending", "On hold", "Irrelevant"])
    )

    hot_count = len(hot_df)
    pot_count = len(pot_df)
    cold_count = len(cold_df)

    col1, col2, col3 = st.columns(3)

    # Person icon instead of pin
    link_col_config = {
        "Profile": st.column_config.LinkColumn("Profile", display_text="👤")
    }

    # HOT PANEL
    with col1:
        hot_header = f"""
            <div style="background-color:#ff6b6b;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                🔥 Hot customers ({hot_count}) — Quoted / Meeting
            </div>
        """
        st.markdown(hot_header, unsafe_allow_html=True)
        if hot_df.empty:
            st.caption("No leads in this group.")
        else:
            st.dataframe(
                hot_df,
                hide_index=True,
                use_container_width=True,
                column_config=link_col_config,
            )

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
                # Hot -> Potential
                if st.button("Move to Potential", key="btn_hot_to_pot"):
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT status FROM contacts WHERE id=?", (selected_hot,)
                    )
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
                # Hot -> Cold
                if st.button("Move to Cold", key="btn_hot_to_cold"):
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT status FROM contacts WHERE id=?", (selected_hot,)
                    )
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
        pot_header = f"""
            <div style="background-color:#28a745;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                🌱 Potential customers ({pot_count}) — New / Contacted
            </div>
        """
        st.markdown(pot_header, unsafe_allow_html=True)
        if pot_df.empty:
            st.caption("No leads in this group.")
        else:
            st.dataframe(
                pot_df,
                hide_index=True,
                use_container_width=True,
                column_config=link_col_config,
            )

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
                # Potential -> Hot (always Meeting)
                if st.button("Move to Hot", key="btn_pot_to_hot"):
                    update_contact_status(conn, selected_pot, "Meeting")
                    st.rerun()
            with c_pot2:
                # Potential -> Cold (New -> Irrelevant, Contacted -> Pending)
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
        cold_header = f"""
            <div style="background-color:#007bff;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                ❄️ Cold customers ({cold_count}) — Pending / On hold / Irrelevant
            </div>
        """
        st.markdown(cold_header, unsafe_allow_html=True)
        if cold_df.empty:
            st.caption("No leads in this group.")
        else:
            st.dataframe(
                cold_df,
                hide_index=True,
                use_container_width=True,
                column_config=link_col_config,
            )

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
                # Cold -> Potential: Contacted
                if st.button("Move to Potential", key="btn_cold_to_pot"):
                    update_contact_status(conn, selected_cold, "Contacted")
                    st.rerun()
            with c_cold2:
                # Cold -> Hot: Meeting
                if st.button("Move to Hot", key="btn_cold_to_hot"):
                    update_contact_status(conn, selected_cold, "Meeting")
                    st.rerun()


# -------------------------------------------------------------
# SIDEBAR IMPORT / EXPORT
# -------------------------------------------------------------
def sidebar_import_export(conn: sqlite3.Connection):
    st.sidebar.header("Import / Export")

    up = st.sidebar.file_uploader(
        "Upload Excel/CSV (Contacts)", type=["xlsx", "xls", "csv"]
    )
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

    st.markdown(
        f"### ✏️ {row['first_name']} {row['last_name']} — {row.get('company') or ''}"
    )
    st.caption(
        f"Status: {row.get('status') or 'New'} | Application: {row.get('application') or '—'}"
    )
    profile_url_header = row.get("profile_url")
    if profile_url_header:
        st.markdown(f"🔗 Profile: [{profile_url_header}]({profile_url_header})")

    with st.form(f"edit_{contact_id}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            first = st.text_input("First name", row["first_name"] or "")
            job = st.text_input("Job title", row["job_title"] or "")
            phone = st.text_input("Phone", row["phone"] or "")

            gender_options = ["", "Female", "Male", "Other"]
            raw_gender = row["gender"] or ""
            current_gender = raw_gender if raw_gender in gender_options else ""
            gender_index = gender_options.index(current_gender)

            gender = st.selectbox(
                "Gender",
                gender_options,
                index=gender_index,
            )
        with col2:
            last = st.text_input("Last name", row["last_name"] or "")
            company = st.text_input("Company", row["company"] or "")
            email = st.text_input("Email", row["email"] or "")
            app_options = [""] + APPLICATIONS
            current_app = row["application"] or ""
            app_index = (
                app_options.index(current_app) if current_app in app_options else 0
            )
            application = st.selectbox("Application", app_options, index=app_index)
        with col3:
            cat_opts = [
                "PhD/Student",
                "Professor/Academic",
                "Academic",
                "Industry",
                "Other",
            ]
            category = st.selectbox(
                "Category",
                cat_opts,
                index=cat_opts.index(row["category"])
                if row["category"] in cat_opts
                else 0,
            )
            status = st.selectbox(
                "Status",
                PIPELINE,
                index=PIPELINE.index(row["status"])
                if row["status"] in PIPELINE
                else 0,
            )

            # Owner dropdown
            existing_owner = (row["owner"] or "").strip()
            owner_choices = OWNER_CHOICES.copy()
            if existing_owner and existing_owner not in owner_choices:
                owner_choices = [existing_owner] + owner_choices
            owner_index = owner_choices.index(existing_owner) if existing_owner in owner_choices else 0
            owner = st.selectbox(
                "Owner",
                owner_choices,
                index=owner_index,
            )

            product_options = [""] + PRODUCTS
            raw_prod = row["product_interest"] or ""
            current_prod = raw_prod if raw_prod in product_options else ""
            prod_index = product_options.index(current_prod)

            product = st.selectbox(
                "Product type interest",
                product_options,
                index=prod_index,
            )

        st.write("**Address**")
        street = st.text_input("Street", row["street"] or "")
        street2 = st.text_input("Street 2", row["street2"] or "")
        city = st.text_input("City", row["city"] or "")
        state = st.text_input("State/Province", row["state"] or "")
        zipc = st.text_input("ZIP", row["zip_code"] or "")
        country = st.text_input("Country", row["country"] or "")
        website = st.text_input("Website", row["website"] or "")
        profile_url = st.text_input("Profile URL", row.get("profile_url") or "")

        col_save, col_delete = st.columns([3, 1])
        with col_save:
            saved = st.form_submit_button("Save changes")
        with col_delete:
            delete_pressed = st.form_submit_button("🗑️ Delete this contact")

        if saved:
            cur = conn.cursor()
            if row["status"] != status:
                cur.execute(
                    "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
                    (contact_id, datetime.utcnow().isoformat(), row["status"], status),
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
                    first,
                    last,
                    job,
                    company,
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
                    (profile_url or "").strip() or None,
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

    with st.expander("🔗 Links"):
        profile_url_view = row.get("profile_url")
        website_view = row.get("website")
        if profile_url_view:
            st.markdown(f"👤 [Profile]({profile_url_view})")
        if website_view:
            st.markdown(f"🌐 [Company website]({website_view})")
        if not profile_url_view and not website_view:
            st.caption("No profile or website saved for this contact.")

    st.markdown("#### 🗒️ Notes")
    note_key = f"note_{contact_id}"
    fu_key = f"nextfu_{contact_id}"

    new_note = st.text_area(
        "Add a note",
        key=note_key,
        placeholder="Called; left voicemail…",
    )
    next_fu = st.date_input(
        "Next follow-up",
        value=st.session_state.get(fu_key, date.today()),
        key=fu_key,
    )

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
                conn.execute(
                    "UPDATE contacts SET last_touch=? WHERE id=?",
                    (ts_iso, contact_id),
                )
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
# MANUAL ADD CONTACT FORM (with initial note + fixed Clear)
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
                gender = st.selectbox(
                    "Gender",
                    ["", "Female", "Male", "Other"],
                    key="add_gender",
                )
            with col2:
                last = st.text_input("Last name", key="add_last")
                company = st.text_input("Company", key="add_company")
                email = st.text_input("Email", key="add_email")
                application_raw = st.selectbox(
                    "Application",
                    [""] + APPLICATIONS,
                    key="add_application",
                )
            with col3:
                cat_opts = [
                    "PhD/Student",
                    "Professor/Academic",
                    "Academic",
                    "Industry",
                    "Other",
                ]
                category = st.selectbox(
                    "Category",
                    cat_opts,
                    index=3,
                    key="add_category",
                )
                status = st.selectbox(
                    "Status",
                    PIPELINE,
                    index=0,
                    key="add_status",
                )
                # Owner dropdown
                owner = st.selectbox(
                    "Owner",
                    OWNER_CHOICES,
                    index=OWNER_CHOICES.index("Liz") if "Liz" in OWNER_CHOICES else 0,
                    key="add_owner",
                )
                product = st.selectbox(
                    "Product type interest",
                    [""] + PRODUCTS,
                    key="add_product",
                )

            st.write("**Address**")
            street = st.text_input("Street", key="add_street")
            street2 = st.text_input("Street 2", key="add_street2")
            city = st.text_input("City", key="add_city")
            state = st.text_input("State/Province", key="add_state")
            zipc = st.text_input("ZIP", key="add_zip")
            country = st.text_input("Country", key="add_country")
            website = st.text_input("Website", key="add_website")
            profile_url = st.text_input("Profile URL", key="add_profile_url")

            # Initial note field (new)
            initial_note = st.text_area(
                "Initial note",
                key="add_note",
                placeholder="How did we meet? What did they say?",
            )

            submitted = st.form_submit_button("Create contact")

        # Separate clear button (outside the form) so it actually resets
        clear = st.button("Clear form")

        if submitted:
            if not email and not (first and last and company):
                st.error(
                    "Please provide either an email, or first name + last name + company."
                )
            else:
                scan_dt = datetime.utcnow().isoformat()
                email_norm = (email or "").strip().lower() or None
                status_norm = normalize_status(status) or "New"
                application_norm = normalize_application(application_raw)

                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO contacts
                    (scan_datetime, first_name, last_name, job_title, company,
                     street, street2, zip_code, city, state, country,
                     phone, email, website, profile_url, category, status, owner, last_touch,
                     gender, application, product_interest)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                        website or None,
                        (profile_url or "").strip() or None,
                        category,
                        status_norm,
                        (owner or "").strip() or None,
                        scan_dt,
                        gender or None,
                        application_norm,
                        product or None,
                    ),
                )
                contact_id = cur.lastrowid

                # Save initial note, if any
                if initial_note and initial_note.strip():
                    ts_iso = scan_dt
                    conn.execute(
                        "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                        (contact_id, ts_iso, initial_note.strip(), None),
                    )

                conn.commit()
                backup_contacts(conn)
                st.success("New contact created")
                st.rerun()

        if clear:
            for key in [
                "add_first",
                "add_job",
                "add_phone",
                "add_gender",
                "add_last",
                "add_company",
                "add_email",
                "add_application",
                "add_category",
                "add_status",
                "add_owner",
                "add_product",
                "add_street",
                "add_street2",
                "add_city",
                "add_state",
                "add_zip",
                "add_country",
                "add_website",
                "add_profile_url",
                "add_note",
            ]:
                st.session_state.pop(key, None)
            st.rerun()


# -------------------------------------------------------------
# MAIN APP
# -------------------------------------------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")

    conn = get_conn()
    init_db(conn)
    restore_from_backup_if_empty(conn)

    # Multi-user 2FA gate
    check_login_two_factor_multiuser(conn)

    # If we're here, user is authenticated
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
        "first_name",
        "last_name",
        "email",
        "phone",
        "job_title",
        "company",
        "city",
        "state",
        "country",
        "website",
        "category",
        "status",
        "owner",
        "gender",
        "application",
        "product_interest",
        "last_touch",
        "notes",
        "profile_url",
    ]
    available_cols = [c for c in export_cols if c in df.columns]

    # CSV export keeps internal column names
    st.session_state["export_df"] = df[available_cols].copy()

    # For on-screen table, make profile + website columns nicer
    display_df = df[available_cols].copy()
    if "profile_url" in display_df.columns:
        display_df = display_df.rename(columns={"profile_url": "Profile URL"})
    if "website" in display_df.columns:
        display_df = display_df.rename(columns={"website": "Website"})

    col_config = {}
    if "Profile URL" in display_df.columns:
        col_config["Profile URL"] = st.column_config.LinkColumn(
            "Profile", display_text="👤"
        )
    if "Website" in display_df.columns:
        col_config["Website"] = st.column_config.LinkColumn(
            "Website", display_text="🌐"
        )

    st.subheader("Contacts")
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config=col_config,
    )

    options = [
        (int(r.id), f"{r.first_name} {r.last_name} — {r.company or ''}")
        for r in df[["id", "first_name", "last_name", "company"]].itertuples(
            index=False
        )
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

