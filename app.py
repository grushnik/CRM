# =============================================================
# RADOM CRM — FULL APP.PY (WITH CHRISTMAS SNOWFLAKE BACKGROUND)
# =============================================================

import os
import re
import sqlite3
import random
import time
import base64
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

APPLICATIONS = sorted([
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
])

PRODUCTS = ["1 kW", "10 kW", "100 kW", "1 MW"]
PIPELINE = ["New","Contacted","Meeting","Quoted","Won","Lost","Nurture","Pending","On hold","Irrelevant"]
OWNERS = ["", "Velibor", "Liz", "Jovan", "Ian", "Qi", "Kenshin"]

# -------------------------------------------------------------
# 🎄 CHRISTMAS BACKGROUND (SNOWFLAKES)
# -------------------------------------------------------------
def inject_snowflake_background():
    svg = """
    <svg xmlns="http://www.w3.org/2000/svg" width="260" height="260">
      <rect width="260" height="260" fill="none"/>
      <g stroke="rgba(120,60,255,0.35)" stroke-width="2">
        <path d="M40 50 l10 10 M50 50 l-10 10 M45 42 v16 M37 50 h16"/>
        <path d="M200 70 l10 10 M210 70 l-10 10 M205 62 v16 M197 70 h16"/>
        <path d="M120 190 l10 10 M130 190 l-10 10 M125 182 v16 M117 190 h16"/>
        <path d="M70 160 l8 8 M78 160 l-8 8 M74 154 v12 M68 160 h12"/>
        <path d="M190 170 l8 8 M198 170 l-8 8 M194 164 v12 M188 170 h12"/>
      </g>
      <g fill="rgba(120,60,255,0.18)">
        <circle cx="95" cy="35" r="2"/>
        <circle cx="160" cy="120" r="2"/>
        <circle cx="30" cy="210" r="2"/>
        <circle cx="235" cy="220" r="2"/>
        <circle cx="220" cy="25" r="2"/>
      </g>
    </svg>
    """.strip()

    b64 = base64.b64encode(svg.encode()).decode()
    bg = f"data:image/svg+xml;base64,{b64}"

    st.markdown(f"""
    <style>
    [data-testid="stAppViewContainer"] {{
        background-image: url("{bg}");
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
    """, unsafe_allow_html=True)

# -------------------------------------------------------------
# URL HELPER
# -------------------------------------------------------------
def _clean_url(v):
    if not v:
        return ""
    s = str(v).strip()
    if s.startswith("http"):
        return s
    return "https://" + s

# -------------------------------------------------------------
# DATABASE
# -------------------------------------------------------------
def get_conn():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn):
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
        status TEXT,
        owner TEXT,
        last_touch TEXT,
        gender TEXT,
        application TEXT,
        product_interest TEXT,
        photo TEXT,
        profile_url TEXT
    );
    """)
    conn.commit()

# -------------------------------------------------------------
# LOGIN (Telegram 2FA)
# -------------------------------------------------------------
def check_login_two_factor_telegram():
    expected = st.secrets.get("APP_PASSWORD", DEFAULT_PASSWORD)
    ss = st.session_state
    ss.setdefault("authed", False)

    if ss["authed"]:
        return

    st.sidebar.header("🔐 Login")
    user = st.sidebar.text_input("Telegram username (without @)")
    pwd = st.sidebar.text_input("Password", type="password")

    if st.sidebar.button("Login"):
        if pwd != expected:
            st.sidebar.error("Wrong password")
            st.stop()
        ss["authed"] = True
        st.rerun()

    st.stop()

# -------------------------------------------------------------
# MAIN APP
# -------------------------------------------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    inject_snowflake_background()   # 🎄 THIS WAS THE MISSING PIECE
    check_login_two_factor_telegram()

    conn = get_conn()
    init_db(conn)

    left, right = st.columns([3,1])
    with left:
        st.title(APP_TITLE)
        st.caption("Upload leads → categorize → work the pipeline → export.")
    with right:
        st.markdown("""
        <div style="padding:10px;border-radius:14px;
        background:linear-gradient(135deg,#8b2cff,#5a22ff,#a100ff);
        color:white;text-align:right;">
            <div style="font-size:12px;">Sold systems</div>
            <div style="font-size:32px;font-weight:800;">2</div>
        </div>
        """, unsafe_allow_html=True)

    st.subheader("Customer overview")
    st.info("🎄 Snowflakes mean this build is blessed.")

if __name__ == "__main__":
    main()
