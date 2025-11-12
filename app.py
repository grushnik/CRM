import os
import io
import re
import sqlite3
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import pandas as pd
import streamlit as st
from dateutil import parser as dtparser

APP_TITLE = "Radom CRM"
DB_FILE = "data/radom_crm.db"

# ------------------------- DB helpers (sqlite3 only) -------------------------

def get_conn() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
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
          last_touch TEXT
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
    conn.commit()

# ------------------------- Import / normalize -------------------------

COLMAP = {
    "scan date/time": "scan_datetime",
    "first name": "first_name",
    "last name": "last_name",
    "job title": "job_title",
    "company": "company",
    "street": "street",
    "street (line 2)": "street2",
    "zip code": "zip_code",
    "city": "city",
    "state/province": "state",
    "state": "state",
    "country": "country",
    "phone": "phone",
    "email": "email",
    "notes": "notes",
    "website": "website",
}

EXPECTED = [
    "scan_datetime", "first_name", "last_name", "job_title", "company",
    "street", "street2", "zip_code", "city", "state", "country",
    "phone", "email", "website", "notes"
]

STUDENT_PAT = re.compile(r"\b(phd|ph\.d|student|undergrad|graduate)\b", re.I)
PROF_PAT = re.compile(r"\b(assistant|associate|full)?\s*professor\b|department chair", re.I)
IND_PAT = re.compile(r"\b(director|manager|engineer|scientist|vp|founder|ceo|cto|lead|principal)\b", re.I)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {c: COLMAP.get(c.strip().lower(), c.strip().lower()) for c in df.columns}
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

def parse_dt(value) -> Optional[str]:
    if pd.isna(value) or value is None or str(value).strip() == "":
        return None
    try:
        return dtparser.parse(str(value)).isoformat()
    except Exception:
        return str(value)

def upsert_contacts(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    df = normalize_columns(df).fillna("")
    df["category"] = df.apply(infer_category, axis=1)
    df["scan_datetime"] = df["scan_datetime"].apply(parse_dt)

    added_or_updated = 0
    cur = conn.cursor()
    for _, r in df.iterrows():
        email = r["email"].strip().lower() or None

        if email:
            cur.execute("SELECT id FROM contacts WHERE email = ?", (email,))
            row = cur.fetchone()
        else:
            cur.execute(
                "SELECT id FROM contacts WHERE first_name=? AND last_name=? AND company=?",
                (r["first_name"], r["last_name"], r["company"]),
            )
            row = cur.fetchone()

        payload = (
            r["scan_datetime"], r["first_name"], r["last_name"], r["job_title"], r["company"],
            r["street"], r["street2"], r["zip_code"], r["city"], r["state"], r["country"],
            str(r["phone"]) if r["phone"] != "" else None, email,
            r["website"], r["category"]
        )

        if row:
            cur.execute(
                """
                UPDATE contacts SET scan_datetime=?, first_name=?, last_name=?, job_title=?, company=?,
                    street=?, street2=?, zip_code=?, city=?, state=?, country=?, phone=?, email=?, website=?, category=?
                WHERE id=?
                """,
                payload + (row[0],)
            )
        else:
            cur.execute(
                """
                INSERT INTO contacts
                (scan_datetime, first_name, last_name, job_title, company, street, street2, zip_code,
                 city, state, country, phone, email, website, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload
            )
        added_or_updated += 1

    conn.commit()
    return added_or_updated

# ------------------------- Queries -------------------------

PIPELINE = ["New", "Contacted", "Meeting", "Quoted", "Won", "Lost", "Nurture"]

def query_contacts(conn: sqlite3.Connection, q: str, cats: List[str], stats: List[str], state_like: str) -> pd.DataFrame:
    sql = "SELECT *, (SELECT MAX(ts) FROM notes n WHERE n.contact_id=c.id) AS last_note_ts FROM contacts c WHERE 1=1"
    params: List[Any] = []
    if q:
        sql += " AND (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ?)"
        like = f"%{q}%"
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
    df = pd.read_sql_query(sql, conn, params=params)
    return df

def get_notes(conn: sqlite3.Connection, contact_id: int) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT ts, body, next_followup FROM notes WHERE contact_id=? ORDER BY ts DESC",
        conn, params=(contact_id,)
    )

# ------------------------- UI bits -------------------------

def sidebar_import_export(conn: sqlite3.Connection):
    st.sidebar.header("Import / Export")

    uploaded = st.sidebar.file_uploader("Upload Excel/CSV", type=["xlsx", "xls", "csv"])
    if uploaded is not None:
        if uploaded.name.lower().endswith(".csv"):
            df = pd.read_csv(uploaded)
        else:
            df = pd.read_excel(uploaded)
        n = upsert_contacts(conn, df)
        st.sidebar.success(f"Imported/updated {n} rows")

    total = pd.read_sql_query("SELECT COUNT(*) as n FROM contacts", conn).iloc[0]["n"]
    st.sidebar.caption(f"Total contacts: **{total}**")

    export_df = st.session_state.get("export_df")
    if isinstance(export_df, pd.DataFrame) and not export_df.empty:
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.sidebar.download_button("Download CSV (filtered)", csv, file_name="radom-crm-export.csv")
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="openpyxl") as wr:
            export_df.to_excel(wr, index=False)
        st.sidebar.download_button("Download Excel (filtered)", xls_buf.getvalue(), file_name="radom-crm-export.xlsx")

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
    return q, cats, stats, st_like

def contact_editor(conn: sqlite3.Connection, row: pd.Series):
    st.markdown("---")
    st.markdown(f"### ✏️ Edit: {row['first_name']} {row['last_name']} — {row['company']}")
    with st.form(f"edit_{int(row['id'])}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            first = st.text_input("First name", row["first_name"] or "")
            job = st.text_input("Job title", row["job_title"] or "")
            phone = st.text_input("Phone", row["phone"] or "")
        with col2:
            last = st.text_input("Last name", row["last_name"] or "")
            company = st.text_input("Company", row["company"] or "")
            email = st.text_input("Email", row["email"] or "")
        with col3:
            cat_opts = ["PhD/Student", "Professor/Academic", "Academic", "Industry", "Other"]
            category = st.selectbox("Category", cat_opts, index=cat_opts.index(row["category"]) if row["category"] in cat_opts else 0)
            status = st.selectbox("Status", PIPELINE, index=PIPELINE.index(row["status"]) if row["status"] in PIPELINE else 0)
            owner = st.text_input("Owner", row["owner"] or "")

        st.write("**Address**")
        street = st.text_input("Street", row["street"] or "")
        street2 = st.text_input("Street 2", row["street2"] or "")
        city = st.text_input("City", row["city"] or "")
        state = st.text_input("State/Province", row["state"] or "")
        zip_code = st.text_input("ZIP", row["zip_code"] or "")
        country = st.text_input("Country", row["country"] or "")
        website = st.text_input("Website", row["website"] or "")

        saved = st.form_submit_button("Save changes")
        if saved:
            cur = conn.cursor()
            # status history if changed
            if row["status"] != status:
                cur.execute(
                    "INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?, ?, ?, ?)",
                    (int(row["id"]), datetime.utcnow().isoformat(), row["status"], status),
                )
            cur.execute(
                """
                UPDATE contacts SET first_name=?, last_name=?, job_title=?, company=?, phone=?, email=?,
                    category=?, status=?, owner=?, street=?, street2=?, city=?, state=?, zip_code=?, country=?,
                    website=?, last_touch=?
                WHERE id=?
                """,
                (
                    first, last, job, company, phone or None, (email or "").lower().strip() or None,
                    category, status, owner or None, street or None, street2 or None, city or None,
                    state or None, zip_code or None, country or None, website or None,
                    datetime.utcnow().isoformat(), int(row["id"])
                )
            )
            conn.commit()
            st.success("Saved")
            st.rerun()

    st.markdown("#### 🗒️ Notes")
    new_note = st.text_area("Add a note", placeholder="Called; left voicemail…")
    next_fu = st.date_input("Next follow-up", value=None)
    if st.button("Add note", key=f"addnote_{int(row['id'])}"):
        if new_note.strip():
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?, ?, ?, ?)",
                (int(row["id"]), datetime.utcnow().isoformat(), new_note.strip(),
                 next_fu.isoformat() if isinstance(next_fu, date) else None),
            )
            cur.execute("UPDATE contacts SET last_touch=? WHERE id=?", (datetime.utcnow().isoformat(), int(row["id"])))
            conn.commit()
            st.success("Note added")
            st.rerun()

    notes_df = get_notes(conn, int(row["id"]))
    st.dataframe(notes_df, use_container_width=True)

# ------------------------- Main app -------------------------

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Upload leads → categorize → work the pipeline → export.")

    conn = get_conn()
    init_db(conn)

    sidebar_import_export(conn)

    q, cats, stats, st_like = filters_ui()
    df = query_contacts(conn, q, cats, stats, st_like)

    if df.empty:
        st.info("No contacts yet — upload an Excel/CSV in the sidebar.")
        return

    export_cols = [
        "first_name","last_name","email","phone","job_title","company",
        "city","state","country","category","status","owner","last_touch"
    ]
    st.session_state["export_df"] = df[export_cols].copy()

    st.subheader("Contacts")
    st.dataframe(df[export_cols], use_container_width=True, hide_index=True)

    options = [(int(r.id), f"{r.first_name} {r.last_name} — {r.company}") for r in df[["id","first_name","last_name","company"]].itertuples(index=False)]
    chosen = st.selectbox("Select a contact to edit", options, format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    if chosen:
        sel_id = chosen[0]
        row = df[df["id"] == sel_id].iloc[0]
        contact_editor(conn, row)

if __name__ == "__main__":
    main()
