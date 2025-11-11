# ⚡ Radom CRM (Streamlit + SQLite)

A lightweight CRM for managing Radom’s conference leads — built with **Streamlit** and **SQLite**.  
Upload your Excel or CSV list, tag and categorize contacts, update their status, add notes, and export filtered data.

---

## 🚀 Features
- 📤 Upload Excel/CSV (auto-maps your column names)
- 🧠 Auto-categorization by job title (PhD/Student, Professor, Industry)
- ✏️ Edit contact details and pipeline status  
  *(New → Contacted → Meeting → Quoted → Won/Lost → Nurture)*
- 🗒️ Add timestamped notes and follow-ups
- 🔍 Search, filter, and export contacts to Excel/CSV
- 💾 Everything stored locally in `data/radom_crm.db`

---

## 🧩 Installation
```bash
# clone the repo
git clone https://github.com/grushnik/CRM.git
cd CRM

# create virtual environment
python -m venv .venv
.venv\Scripts\activate      # (Windows)
# or
source .venv/bin/activate   # (Mac/Linux)

# install dependencies
pip install -r requirements.txt
