import os, io, re, sqlite3
from datetime import datetime, date
from typing import List, Any, Optional

import pandas as pd
import streamlit as st
from dateutil import parser as dtparser

APP_TITLE = "Radom CRM"
DB_FILE = "data/radom_crm.db"

# -------------------- Auth --------------------
DEFAULT_PASSWORD = "CatJorge"

def check_login():
    # Prefer Streamlit Cloud secret if set; else use default
    expected = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)
    if "authed" not in st.session_state:
        st.session_state.authed = False
    if not st.session_state.authed:
        st.sidebar.header("🔐 Login")
        pwd = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Enter"):
            if pwd == expected:
                st.session_state.authed = True
                st.experimental_rerun()
            else:
                st.sidebar.error("Wrong password")
        st.stop()

# -------------------- DB helpers --------------------
def get_conn() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/images", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
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
      photo TEXT
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
    """)
    conn.commit()

# -------------------- Import / normalize --------------------
COLMAP = {
    "scan date/time":"scan_datetime","first name":"first_name","last name":"last_name",
    "job title":"job_title","company":"company","street":"street","street (line 2)":"street2",
    "zip code":"zip_code","city":"city","state/province":"state","state":"state",
    "country":"country","phone":"phone","email":"email","notes":"notes","website":"website",
    "gender":"gender","application":"application","product interest":"product_interest",
    "product_type_interest":"product_interest"
}
EXPECTED = ["scan_datetime","first_name","last_name","job_title","company","street","street2",
            "zip_code","city","state","country","phone","email","website","notes",
            "gender","application","product_interest"]

STUDENT_PAT = re.compile(r"\b(phd|ph\.d|student|undergrad|graduate)\b", re.I)
PROF_PAT    = re.compile(r"\b(assistant|associate|full)?\s*professor\b|department chair", re.I)
IND_PAT     = re.compile(r"\b(director|manager|engineer|scientist|vp|founder|ceo|cto|lead|principal)\b", re.I)

APPLICATIONS = [
    "PFAS destruction","CO2 conversion","Waste-to-Energy","NOx production",
    "Hydrogen production","Carbon black production","Mining waste"
]
PRODUCTS = ["1 kW","10 kW","100 kW","1 MW"]

PIPELINE = ["New","Contacted","Meeting","Quoted","Won","Lost","Nurture","Pending","On hold","Irrelevant"]

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
    if STUDENT_PAT.search(title):      return "PhD/Student"
    if PROF_PAT.search(title):         return "Professor/Academic"
    if any(x in domain for x in (".edu",".ac.","ac.uk",".edu.",".ac.nz",".ac.in")): return "Academic"
    if IND_PAT.search(title):          return "Industry"
    return "Other"

def parse_dt(v) -> Optional[str]:
    if v is None or str(v).strip()=="" or pd.isna(v): return None
    try: return dtparser.parse(str(v)).isoformat()
    except: return str(v)

def upsert_contacts(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    df = normalize_columns(df).fillna("")
    df["category"] = df.apply(infer_category, axis=1)
    df["scan_datetime"] = df["scan_datetime"].apply(parse_dt)
    n = 0
    cur = conn.cursor()
    for _, r in df.iterrows():
        email = r["email"].strip().lower() or None
        if email:
            cur.execute("SELECT id FROM contacts WHERE email=?", (email,))
            row = cur.fetchone()
        else:
            cur.execute("SELECT id FROM contacts WHERE first_name=? AND last_name=? AND company=?",
                        (r["first_name"], r["last_name"], r["company"]))
            row = cur.fetchone()
        payload = (
            r["scan_datetime"], r["first_name"], r["last_name"], r["job_title"], r["company"],
            r["street"], r["street2"], r["zip_code"], r["city"], r["state"], r["country"],
            str(r["phone"]) if r["phone"]!="" else None, email,
            r["website"], r["category"], r.get("gender") or None,
            r.get("application") or None, r.get("product_interest") or None
        )
        if row:
            cur.execute("""
              UPDATE contacts SET scan_datetime=?, first_name=?, last_name=?, job_title=?, company=?,
                street=?, street2=?, zip_code=?, city=?, state=?, country=?, phone=?, email=?, website=?,
                category=?, gender=?, application=?, product_interest=?
              WHERE id=?""", payload+(row[0],))
        else:
            cur.execute("""
              INSERT INTO contacts (scan_datetime,first_name,last_name,job_title,company,street,street2,zip_code,
                                    city,state,country,phone,email,website,category,gender,application,product_interest)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
        n += 1
    conn.commit()
    return n

# -------------------- Queries --------------------
def query_contacts(conn, q: str, cats: List[str], stats: List[str], st_like: str) -> pd.DataFrame:
    sql = "SELECT *, (SELECT MAX(ts) FROM notes n WHERE n.contact_id=c.id) AS last_note_ts FROM contacts c WHERE 1=1"
    params: List[Any] = []
    if q:
        like = f"%{q}%"
        sql += " AND (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ?)"
        params += [like, like, like, like]
    if cats:
        sql += " AND category IN (" + ",".join("?" for _ in cats) + ")"; params += cats
    if stats:
        sql += " AND status IN (" + ",".join("?" for _ in stats) + ")"; params += stats
    if st_like:
        sql += " AND state LIKE ?"; params.append(f"%{st_like}%")
    return pd.read_sql_query(sql, conn, params=params)

def get_notes(conn, contact_id: int) -> pd.DataFrame:
    return pd.read_sql_query("SELECT ts, body, next_followup FROM notes WHERE contact_id=? ORDER BY ts DESC",
                             conn, params=(contact_id,))

# -------------------- Sidebar I/O --------------------
def sidebar_import_export(conn):
    st.sidebar.header("Import / Export")

    up = st.sidebar.file_uploader("Upload Excel/CSV", type=["xlsx","xls","csv"])
    if up is not None:
        df = pd.read_csv(up) if up.name.lower().endswith(".csv") else pd.read_excel(up)
        n = upsert_contacts(conn, df)
        st.sidebar.success(f"Imported/updated {n} rows")

    # Bulk NOTES import (email, body, ts?, next_followup?)
    st.sidebar.markdown("**Bulk import notes**")
    upn = st.sidebar.file_uploader("Upload Notes Excel/CSV", type=["xlsx","xls","csv"], key="notesu")
    if upn is not None:
        notes_df = pd.read_csv(upn) if upn.name.lower().endswith(".csv") else pd.read_excel(upn)
        # expected columns: email, body, ts (optional), next_followup (optional)
        required = {"email","body"}
        if not required.issubset({c.strip().lower() for c in notes_df.columns}):
            st.sidebar.error("Notes file must have at least columns: email, body")
        else:
            # normalize
            notes_df.columns = [c.strip().lower() for c in notes_df.columns]
            cur = conn.cursor()
            added = 0
            for _, r in notes_df.iterrows():
                email = (r.get("email") or "").strip().lower()
                body = (r.get("body") or "").strip()
                ts = r.get("ts")
                fu = r.get("next_followup")
                if not email or not body: continue
                cur.execute("SELECT id FROM contacts WHERE email=?", (email,))
                row = cur.fetchone()
                if not row: continue
                ts_iso = None
                if pd.notna(ts) and str(ts).strip()!="":
                    try: ts_iso = dtparser.parse(str(ts)).isoformat()
                    except: ts_iso = datetime.utcnow().isoformat()
                else:
                    ts_iso = datetime.utcnow().isoformat()
                fu_iso = None
                if pd.notna(fu) and str(fu).strip()!="":
                    try: fu_iso = dtparser.parse(str(fu)).date().isoformat()
                    except: fu_iso = None
                cur.execute("INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                            (int(row[0]), ts_iso, body, fu_iso))
                cur.execute("UPDATE contacts SET last_touch=? WHERE id=?", (ts_iso, int(row[0])))
                added += 1
            conn.commit()
            st.sidebar.success(f"Imported {added} notes")

    total = pd.read_sql_query("SELECT COUNT(*) n FROM contacts", conn).iloc[0]["n"]
    st.sidebar.caption(f"Total contacts: **{total}**")

    # Export filtered contacts + notes (two-sheet Excel)
    export_df = st.session_state.get("export_df")
    notes_export = st.session_state.get("notes_export")
    if isinstance(export_df, pd.DataFrame) and not export_df.empty:
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.sidebar.download_button("Download Contacts CSV (filtered)", csv, file_name="radom-contacts.csv")
        xls = io.BytesIO()
        with pd.ExcelWriter(xls, engine="openpyxl") as wr:
            export_df.to_excel(wr, index=False, sheet_name="Contacts")
            if isinstance(notes_export, pd.DataFrame):
                notes_export.to_excel(wr, index=False, sheet_name="Notes")
        st.sidebar.download_button("Download Excel (Contacts + Notes)", xls.getvalue(),
                                   file_name="radom-crm-export.xlsx")

# -------------------- Filters --------------------
def filters_ui():
    st.subheader("Filters")
    q = st.text_input("Search (name, email, company)", "")
    c1, c2, c3 = st.columns(3)
    with c1:
        cats = st.multiselect("Category", ["PhD/Student","Professor/Academic","Academic","Industry","Other"], [])
    with c2:
        stats = st.multiselect("Status", PIPELINE, [])
    with c3:
        st_like = st.text_input("State/Province contains", "")
    return q, cats, stats, st_like

# -------------------- Photo helpers --------------------
def save_photo(contact_id: int, uploaded_file) -> str:
    ext = os.path.splitext(uploaded_file.name)[1].lower()
    if ext not in [".png",".jpg",".jpeg",".webp"]: ext = ".png"
    path = f"data/images/{contact_id}{ext}"
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path

# -------------------- Editor --------------------
def contact_editor(conn, row: pd.Series):
    st.markdown("---")
    st.markdown(f"### ✏️ Edit: {row['first_name']} {row['last_name']} — {row['company'] or ''}")

    with st.form(f"edit_{int(row['id'])}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            first = st.text_input("First name", row["first_name"] or "")
            job   = st.text_input("Job title", row["job_title"] or "")
            phone = st.text_input("Phone", row["phone"] or "")
            gender = st.selectbox("Gender", ["","Female","Male","Other"], index=["","Female","Male","Other"].index(row["gender"] or ""))
        with col2:
            last   = st.text_input("Last name", row["last_name"] or "")
            company= st.text_input("Company", row["company"] or "")
            email  = st.text_input("Email", row["email"] or "")
            application = st.selectbox("Application", [""]+APPLICATIONS,
                                       index=([""]+APPLICATIONS).index(row["application"] or ""))
        with col3:
            cat_opts = ["PhD/Student","Professor/Academic","Academic","Industry","Other"]
            category = st.selectbox("Category", cat_opts,
                                    index=cat_opts.index(row["category"]) if row["category"] in cat_opts else 0)
            status = st.selectbox("Status", PIPELINE,
                                  index=PIPELINE.index(row["status"]) if row["status"] in PIPELINE else 0)
            owner = st.text_input("Owner", row["owner"] or "")
            product = st.selectbox("Product type interest", [""]+PRODUCTS,
                                   index=([""]+PRODUCTS).index(row["product_interest"] or ""))

        st.write("**Address**")
        street  = st.text_input("Street", row["street"] or "")
        street2 = st.text_input("Street 2", row["street2"] or "")
        city    = st.text_input("City", row["city"] or "")
        state   = st.text_input("State/Province", row["state"] or "")
        zipc    = st.text_input("ZIP", row["zip_code"] or "")
        country = st.text_input("Country", row["country"] or "")
        website = st.text_input("Website", row["website"] or "")

        saved = st.form_submit_button("Save changes")
        if saved:
            cur = conn.cursor()
            if row["status"] != status:
                cur.execute("INSERT INTO status_history(contact_id, ts, old_status, new_status) VALUES (?,?,?,?)",
                            (int(row["id"]), datetime.utcnow().isoformat(), row["status"], status))
            cur.execute("""
                UPDATE contacts SET first_name=?, last_name=?, job_title=?, company=?, phone=?, email=?,
                    category=?, status=?, owner=?, street=?, street2=?, city=?, state=?, zip_code=?, country=?,
                    website=?, last_touch=?, gender=?, application=?, product_interest=?
                WHERE id=?""",
                (first, last, job, company, phone or None, (email or "").lower().strip() or None,
                 category, status, owner or None, street or None, street2 or None, city or None,
                 state or None, zipc or None, country or None, website or None,
                 datetime.utcnow().isoformat(), gender or None, application or None, product or None,
                 int(row["id"]))
            )
            conn.commit()
            st.success("Saved")
            st.rerun()

    # Photo
    with st.expander("📷 Photo"):
        ph_path = row.get("photo")
        if ph_path and os.path.exists(ph_path):
            st.image(ph_path, width=160)
        up = st.file_uploader("Upload/replace photo", type=["png","jpg","jpeg","webp"], key=f"photo_{row['id']}")
        if up is not None:
            saved_path = save_photo(int(row["id"]), up)
            conn.execute("UPDATE contacts SET photo=? WHERE id=?", (saved_path, int(row["id"])))
            conn.commit()
            st.success("Photo saved")
            st.rerun()

    # Notes
    st.markdown("#### 🗒️ Notes")
    new_note = st.text_area("Add a note", placeholder="Called; left voicemail…")
    next_fu  = st.date_input("Next follow-up", value=None)
    if st.button("Add note", key=f"addnote_{int(row['id'])}"):
        if new_note.strip():
            ts_iso = datetime.utcnow().isoformat()
            fu_iso = next_fu.isoformat() if isinstance(next_fu, date) else None
            conn.execute("INSERT INTO notes(contact_id, ts, body, next_followup) VALUES (?,?,?,?)",
                         (int(row["id"]), ts_iso, new_note.strip(), fu_iso))
            conn.execute("UPDATE contacts SET last_touch=? WHERE id=?", (ts_iso, int(row["id"])))
            conn.commit()
            st.success("Note added")
            st.rerun()

    notes_df = get_notes(conn, int(row["id"]))
    st.dataframe(notes_df, use_container_width=True)

# -------------------- App --------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    check_login()

    st.title(APP_TITLE)
    st.caption("Upload leads → categorize → work the pipeline → export.")

    conn = get_conn(); init_db(conn)
    sidebar_import_export(conn)

    q, cats, stats, st_like = filters_ui()
    df = query_contacts(conn, q, cats, stats, st_like)

    if df.empty:
        st.info("No contacts yet — upload an Excel/CSV in the sidebar.")
        return

    export_cols = [
        "first_name","last_name","email","phone","job_title","company","city","state","country",
        "category","status","owner","gender","application","product_interest","last_touch"
    ]
    st.session_state["export_df"] = df[export_cols].copy()

    # Notes export (for all selected rows)
    ids = df["id"].tolist()
    notes_list = []
    for cid in ids:
        ndf = get_notes(conn, int(cid))
        ndf.insert(0, "contact_id", cid)
        notes_list.append(ndf)
    st.session_state["notes_export"] = pd.concat(notes_list, ignore_index=True) if notes_list else pd.DataFrame()

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
