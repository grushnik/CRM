import os
import re
import sqlite3
import random
import time
from datetime import datetime, date
from typing import List, Any, Optional, Dict

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

# -------------------------------------------------------------
# COUNTRY -> ISO2 MAP (for flag images)
# -------------------------------------------------------------
_COUNTRY_TO_ISO2: Dict[str, str] = {
    # Common ones (add more anytime)
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
    "netherlands": "NL",
    "belgium": "BE",
    "switzerland": "CH",
    "austria": "AT",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "finland": "FI",
    "poland": "PL",
    "czech republic": "CZ",
    "czechia": "CZ",
    "slovakia": "SK",
    "ukraine": "UA",
    "russia": "RU",
    "india": "IN",
    "china": "CN",
    "japan": "JP",
    "south korea": "KR",
    "korea": "KR",
    "taiwan": "TW",
    "singapore": "SG",
    "australia": "AU",
    "new zealand": "NZ",
    "israel": "IL",
    "saudi arabia": "SA",
    "united arab emirates": "AE",
    "uae": "AE",
    "turkey": "TR",
}

def _clean_url(url: Any) -> Optional[str]:
    """Ensure URL is clickable. If missing scheme, prepend https://"""
    if url is None:
        return None
    s = str(url).strip()
    if not s:
        return None
    if s.lower().startswith(("http://", "https://")):
        return s
    return "https://" + s

def flag_img(country: Any, size: int = 18) -> str:
    iso = ""
    if country is None:
        return ""
    s = str(country).strip()
    if not s:
        return ""
    if len(s) == 2 and s.isalpha():
        iso = s.upper()
    else:
        iso = _COUNTRY_TO_ISO2.get(s.lower(), "")
    if not iso:
        return ""
    # FlagCDN is a simple static CDN for flags
    return (
        f"<img src='https://flagcdn.com/{iso.lower()}.svg' "
        f"width='{size}' "
        f"style='vertical-align:middle;border-radius:2px;margin-left:6px;'/>"
    )

# -------------------------------------------------------------
# TELEGRAM OTP (2-FACTOR) - ORIGINAL SINGLE CHAT VERSION
# (You can keep your current working one; not changing it here.)
# -------------------------------------------------------------
def _send_telegram_otp(code: str):
    token = st.secrets.get("TELEGRAM_BOT_TOKEN")
    chat_id = st.secrets.get("ADMIN_CHAT_ID") or st.secrets.get("TELEGRAM_CHAT_ID")

    if not (token and chat_id):
        st.sidebar.warning(
            f"⚠️ Telegram secrets not configured. Use this one-time code: **{code}**"
        )
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = f"Radom CRM login code: {code} (valid {OTP_TTL_SECONDS//60} min)"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        if resp.status_code == 200:
            # IMPORTANT: you asked to remove the temporary 6-digit display;
            # We won't display the code here unless Telegram fails.
            st.sidebar.success("✅ Code sent to Telegram.")
        else:
            st.sidebar.error(f"Telegram error {resp.status_code}: {resp.text}")
            st.sidebar.info(f"Use this one-time code instead: **{code}**")
    except Exception as e:
        st.sidebar.error(f"Could not send Telegram message: {e}")
        st.sidebar.info(f"Use this one-time code instead: **{code}**")

def check_login_two_factor_telegram():
    expected = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)

    ss = st.session_state
    ss.setdefault("auth_pw_ok", False)
    ss.setdefault("authed", False)

    if ss["authed"]:
        return

    st.sidebar.header("🔐 Login")

    if not ss["auth_pw_ok"]:
        pwd = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Continue"):
            if pwd == expected:
                ss["auth_pw_ok"] = True
                code = f"{random.randint(0, 999999):06d}"
                ss["otp_code"] = code
                ss["otp_time"] = int(time.time())
                _send_telegram_otp(code)
                st.rerun()
            else:
                st.sidebar.error("Wrong password")
        st.stop()

    if "otp_time" in ss and int(time.time()) - ss["otp_time"] > OTP_TTL_SECONDS:
        for k in ("auth_pw_ok", "otp_code", "otp_time"):
            ss.pop(k, None)
        st.sidebar.error("Code expired. Please start over.")
        st.stop()

    st.sidebar.caption("Enter the 6-digit code sent to Telegram.")
    code_in = st.sidebar.text_input("Enter 6-digit code", max_chars=6)
    if st.sidebar.button("Verify"):
        if code_in.strip() == ss.get("otp_code", ""):
            ss["authed"] = True
            for k in ("auth_pw_ok", "otp_code", "otp_time"):
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
    if "last_touch" not in cols:
        cur.execute("ALTER TABLE contacts ADD COLUMN last_touch TEXT")
    if "website" not in cols:
        cur.execute("ALTER TABLE contacts ADD COLUMN website TEXT")
    if "country" not in cols:
        cur.execute("ALTER TABLE contacts ADD COLUMN country TEXT")

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
    "owner": "owner",
    "photo": "photo",
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
        "website",
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
        profile_url = _clean_url(r.get("profile_url") or None)
        website = _clean_url(r.get("website") or None)
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
                website,
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
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
# TOP PRIORITY LISTS (HOT / POTENTIAL / COLD) WITH FLAG IMAGES
# -------------------------------------------------------------
def _render_lead_list(title_html: str, df: pd.DataFrame):
    st.markdown(title_html, unsafe_allow_html=True)

    if df.empty:
        st.caption("No leads in this group.")
        return

    # Render each lead as a compact row
    for _, sub in df.iterrows():
        lead = f"{(sub.get('first_name') or '').strip()} {(sub.get('last_name') or '').strip()}".strip()
        flag = flag_img(sub.get("country"))
        profile = _clean_url(sub.get("profile_url") or "")
        company = (sub.get("company") or "").strip()
        email = (sub.get("email") or "").strip()
        status = (sub.get("status") or "").strip()

        # If no profile URL, don't make a broken link
        profile_icon = "👤"
        if profile:
            profile_icon_html = f"<a href='{profile}' target='_blank' style='text-decoration:none;'>{profile_icon}</a>"
        else:
            profile_icon_html = f"<span>{profile_icon}</span>"

        row_html = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.08);">
          <div style="display:flex;gap:10px;align-items:center;">
            {profile_icon_html}
            <div>
              <div style="font-weight:600;">{lead} {flag}</div>
              <div style="font-size:12px;opacity:0.75;">{company} • {email}</div>
            </div>
          </div>
          <div style="font-size:12px;opacity:0.85;">{status}</div>
        </div>
        """
        st.markdown(row_html, unsafe_allow_html=True)

def show_priority_lists(conn: sqlite3.Connection):
    st.subheader("Customer overview")

    df_all = pd.read_sql_query(
        "SELECT id, first_name, last_name, company, email, status, profile_url, country FROM contacts",
        conn,
    )

    if df_all.empty:
        st.caption("No contacts yet – add someone manually or import a file.")
        return

    df_all["status"] = df_all["status"].fillna("New").astype(str).str.strip()

    hot_df = df_all[df_all["status"].isin(["Quoted", "Meeting"])].copy()
    pot_df = df_all[df_all["status"].isin(["New", "Contacted"])].copy()
    cold_df = df_all[df_all["status"].isin(["Pending", "On hold", "Irrelevant"])].copy()

    col1, col2, col3 = st.columns(3)

    with col1:
        _render_lead_list(
            f"""
            <div style="background-color:#ff6b6b;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                🔥 Hot customers ({len(hot_df)}) — Quoted / Meeting
            </div>
            """,
            hot_df,
        )

    with col2:
        _render_lead_list(
            f"""
            <div style="background-color:#28a745;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                🌱 Potential customers ({len(pot_df)}) — New / Contacted
            </div>
            """,
            pot_df,
        )

    with col3:
        _render_lead_list(
            f"""
            <div style="background-color:#007bff;padding:6px 10px;border-radius:8px;
                        font-weight:600;color:white;text-align:center;margin-bottom:6px;">
                ❄️ Cold customers ({len(cold_df)}) — Pending / On hold / Irrelevant
            </div>
            """,
            cold_df,
        )

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
        f"Status: {row.get('status') or 'New'} | Application: {row.get('application') or '—'} | Owner: {row.get('owner') or '—'}"
    )

    # --- CHANGE #1: Profile + Website under it (header links) ---
    profile_url_header = row.get("profile_url")
    if profile_url_header:
        u = _clean_url(profile_url_header)
        if u:
            st.markdown(f"🔗 Profile: [{u}]({u})")

    website_header = row.get("website")
    if website_header:
        w = _clean_url(website_header)
        if w:
            st.markdown(f"🌐 Website: [{w}]({w})")

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
            gender = st.selectbox("Gender", gender_options, index=gender_index)

        with col2:
            last = st.text_input("Last name", row["last_name"] or "")
            company = st.text_input("Company", row["company"] or "")
            email = st.text_input("Email", row["email"] or "")

            app_options = [""] + APPLICATIONS
            current_app = row["application"] or ""
            app_index = app_options.index(current_app) if current_app in app_options else 0
            application = st.selectbox("Application", app_options, index=app_index)

        with col3:
            cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
            category = st.selectbox(
                "Category",
                cat_opts,
                index=cat_opts.index(row["category"]) if row["category"] in cat_opts else 0,
            )
            status = st.selectbox(
                "Status",
                PIPELINE,
                index=PIPELINE.index(row["status"]) if row["status"] in PIPELINE else 0,
            )

            product_options = [""] + PRODUCTS
            raw_prod = row["product_interest"] or ""
            current_prod = raw_prod if raw_prod in product_options else ""
            prod_index = product_options.index(current_prod)

            owner = st.text_input("Owner", row["owner"] or "")
            product = st.selectbox("Product type interest", product_options, index=prod_index)

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
                    _clean_url(website),
                    _clean_url(profile_url),
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
# MANUAL ADD CONTACT FORM (unchanged from your base)
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
                owner = st.text_input("Owner", key="add_owner")
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
                            _clean_url(website),
                            _clean_url(profile_url),
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
                    "add_first","add_job","add_phone","add_gender",
                    "add_last","add_company","add_email","add_application",
                    "add_category","add_status","add_owner","add_product",
                    "add_street","add_street2","add_city","add_state","add_zip",
                    "add_country","add_website","add_profile_url",
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

    # --- CHANGE #2: Hot/Potential/Cold uses HTML rows + flags ---
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
    st.session_state["export_df"] = df[available_cols].copy()

    display_df = df[available_cols].copy()
    if "profile_url" in display_df.columns:
        display_df = display_df.rename(columns={"profile_url": "Profile URL"})

    st.subheader("Contacts")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

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
