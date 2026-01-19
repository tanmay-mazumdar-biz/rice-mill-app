import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import hashlib
from datetime import datetime, date, timedelta
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

# Google Sheets setup
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource
def get_google_sheets_client():
    """Initialize Google Sheets client"""
    try:
        credentials = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=SCOPES
        )
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        return None

# Page configuration
st.set_page_config(
    page_title="Rice Mill System (Cloud)",
    page_icon="🌾",
    layout="wide"
)

# --- CONFIGURATION ---
SHEET_NAME = "Rice Mill Database"  # Make sure your Google Sheet has this EXACT name
WEIGHT_PER_BAG = 0.51
DIFFERENCE_THRESHOLD = 2.0

# --- GOOGLE SHEETS CONNECTION ---
def get_gsheet_connection():
    """Connect to Google Sheets using Streamlit Secrets"""
    # Create a credentials object from the secrets dictionary
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # We construct the creds dict from st.secrets
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    
    return client.open(SHEET_NAME)

def get_data(worksheet_name):
    """Fetch all records as DataFrame"""
    try:
        sh = get_gsheet_connection()
        worksheet = sh.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        return df
    except gspread.WorksheetNotFound:
        st.error(f"❌ Worksheet '{worksheet_name}' not found!")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return pd.DataFrame()

# --- HELPER FUNCTIONS ---
def get_kms_year_from_date(entry_date):
    if isinstance(entry_date, str):
        entry_date = datetime.strptime(entry_date, "%Y-%m-%d").date()
    year = entry_date.year
    month = entry_date.month
    if month >= 10:
        kms_start = year
    else:
        kms_start = year - 1
    return f"{kms_start}-{str(kms_start + 1)[-2:]}"

def get_current_kms_year():
    return get_kms_year_from_date(date.today())

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

# --- DATA LOGIC ---

def verify_login(username, password):
    df = get_data("Users")
    if df.empty: return None
    hashed = hash_password(password)
    # Ensure columns exist to prevent errors
    if 'username' not in df.columns: return None
    
    user = df[(df['username'] == username) & (df['password_hash'] == hashed)]
    if not user.empty:
        row = user.iloc[0]
        # Return tuple: (fake_id, username, role, full_name)
        return (999, row['username'], row['role'], row['full_name'])
    return None

def update_stock_logic(kms_year):
    """Re-calculate master stock and OVERWRITE the sheet"""
    try:
        # 1. Fetch Data
        admin_df = get_data("Admin_Arrivals")
        stock_df = get_data("Master_Stock")
        
        if admin_df.empty:
            return True # Nothing to calculate

        # Filter for current year
        admin_df = admin_df[admin_df['kms_year'] == kms_year]
        
        # 2. Capture Manual Edits (Issued Milling & Remarks)
        manual_data = {}
        if not stock_df.empty:
            stock_df = stock_df[stock_df['kms_year'] == kms_year]
            for _, row in stock_df.iterrows():
                manual_data[str(row['date'])] = {
                    'issued': row.get('issued_milling', 0),
                    'remarks': row.get('remarks', '')
                }

        # 3. Calculate Daily Sums
        daily_sums = admin_df.groupby('date')['quantity_quintals'].sum().reset_index()
        daily_sums = daily_sums.sort_values('date')

        # 4. Build New Rows
        new_rows = []
        prog_received = 0
        prog_milling = 0
        prev_closing = 0
        
        # Headers for the sheet
        headers = ['date', 'kms_year', 'opening_balance', 'received_today', 'prog_received', 
                   'total', 'issued_milling', 'prog_milling', 'closing_balance', 'remarks']

        for _, row in daily_sums.iterrows():
            d_date = str(row['date'])
            received = float(row['quantity_quintals'])
            
            # Retrieve manual values
            issued = float(manual_data.get(d_date, {}).get('issued', 0))
            remarks = str(manual_data.get(d_date, {}).get('remarks', ''))
            
            prog_received += received
            prog_milling += issued
            
            opening = prev_closing
            total = opening + received
            closing = total - issued
            prev_closing = closing
            
            new_rows.append([d_date, kms_year, opening, received, prog_received, 
                             total, issued, prog_milling, closing, remarks])

        # 5. Write to Sheet (Clear & Replace)
        sh = get_gsheet_connection()
        ws = sh.worksheet("Master_Stock")
        ws.clear()
        ws.append_row(headers)
        if new_rows:
            ws.append_rows(new_rows)
            
        return True
    except Exception as e:
        st.error(f"Stock Update Failed: {e}")
        return False

# --- UI COMPONENTS ---

def show_login():
    st.markdown("<h1 style='text-align: center;'>🌾 Rice Mill (Cloud)</h1>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login"):
            user = st.text_input("Username")
            pwd = st.text_input("Password", type="password")
            if st.form_submit_button("Login", use_container_width=True):
                user_data = verify_login(user, pwd)
                if user_data:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = user_data[1]
                    st.session_state['role'] = user_data[2]
                    st.session_state['full_name'] = user_data[3]
                    st.rerun()
                else:
                    st.error("Invalid Login")

def show_dashboard():
    # Sidebar
    st.sidebar.title(f"👤 {st.session_state['full_name']}")
    st.sidebar.write(f"Role: **{st.session_state['role'].upper()}**")
    
    current_kms = get_current_kms_year()
    years = [f"{y}-{str(y+1)[-2:]}" for y in range(int(current_kms[:4])-1, int(current_kms[:4])+2)]
    kms_year = st.sidebar.selectbox("KMS Year", years, index=1)
    
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

    # Main Area
    if st.session_state['role'] == 'admin':
        admin_portal(kms_year)
    else:
        employee_portal(kms_year)

# --- PORTALS ---

def employee_portal(kms_year):
    st.title("👷 Employee Portal")
    tab1, tab2 = st.tabs(["New Entry", "My Entries"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            date_val = st.date_input("Date")
            mandi_df = get_data("Settings_Mandi")
            mandis = mandi_df['mandi_name'].tolist() if not mandi_df.empty else []
            mandi = st.selectbox("Mandi", mandis)
            bags = st.number_input("Bags", min_value=1, step=1)
        
        with col2:
            veh_df = get_data("Vehicle_Registry")
            # Filter active vehicles
            if not veh_df.empty and 'is_active' in veh_df.columns:
                vehs = veh_df[veh_df['is_active'] == 1]['vehicle_number'].tolist()
            else:
                vehs = veh_df['vehicle_number'].tolist() if not veh_df.empty else []
                
            vehicle = st.selectbox("Vehicle", vehs)
            weight = st.number_input("Weight (Quintals)", min_value=0.0)
            
            godown_df = get_data("Settings_Godown")
            godowns = godown_df['godown_name'].tolist() if not godown_df.empty else []
            godown = st.selectbox("Godown", godowns)

        # Calculation
        expected = round(bags * WEIGHT_PER_BAG, 2)
        diff = round(weight - expected, 2)
        
        st.info(f"Expected: {expected} Q | Actual: {weight} Q | Diff: {diff} Q")
        
        if st.button("Submit Entry", type="primary"):
            sh = get_gsheet_connection()
            ws = sh.worksheet("Employee_Arrivals")
            # date, kms_year, mandi_name, vehicle_number, bags, weight_quintals, godown, expected_weight, difference, entered_by, remarks
            ws.append_row([str(date_val), kms_year, mandi, vehicle, bags, weight, godown, expected, diff, st.session_state['username'], ""])
            st.success("Saved to Google Sheets!")

    with tab2:
        df = get_data("Employee_Arrivals")
        if not df.empty:
            df = df[df['entered_by'] == st.session_state['username']]
            st.dataframe(df)

def admin_portal(kms_year):
    st.title("🔑 Admin Portal")
    tabs = st.tabs(["Entry", "Stock", "Vehicles", "Settings"])
    
    # 1. ADMIN ENTRY
    with tabs[0]:
        c1, c2 = st.columns([1, 2])
        with c1:
            st.subheader("Add Arrival")
            a_date = st.date_input("Date", key="ad_date")
            
            mandi_df = get_data("Settings_Mandi")
            mandi_list = mandi_df['mandi_name'].tolist() if not mandi_df.empty else []
            a_mandi = st.selectbox("Mandi", mandi_list, key="ad_mandi")
            
            veh_df = get_data("Vehicle_Registry")
            veh_list = veh_df['vehicle_number'].tolist() if not veh_df.empty else []
            a_veh = st.selectbox("Vehicle", veh_list, key="ad_veh")
            
            a_note = st.text_input("AC Note No")
            a_qty = st.number_input("Quantity (Q)", min_value=0.1)
            
            if st.button("Save Arrival"):
                sh = get_gsheet_connection()
                ws = sh.worksheet("Admin_Arrivals")
                # date, kms_year, mandi_name, vehicle_number, ac_note, quantity_quintals, entered_by, remarks
                ws.append_row([str(a_date), kms_year, a_mandi, a_veh, a_note, a_qty, st.session_state['username'], ""])
                update_stock_logic(kms_year)
                st.success("Saved & Stock Updated!")
                st.rerun()

        with c2:
            st.subheader("Recent Entries")
            df = get_data("Admin_Arrivals")
            if not df.empty:
                df = df[df['kms_year'] == kms_year]
                # Add index for deletion (Sheet row = index + 2)
                df['sheet_row'] = df.index + 2 
                st.dataframe(df[['date', 'mandi_name', 'vehicle_number', 'quantity_quintals']])
                
                # Delete Logic
                row_to_del = st.number_input("Row ID to Delete (Check Sheet)", min_value=2, step=1)
                if st.button("Delete Row"):
                    sh = get_gsheet_connection()
                    ws = sh.worksheet("Admin_Arrivals")
                    ws.delete_rows(int(row_to_del))
                    update_stock_logic(kms_year)
                    st.warning("Deleted!")
                    st.rerun()

    # 2. STOCK
    with tabs[1]:
        if st.button("Force Recalculate Stock"):
            update_stock_logic(kms_year)
            st.success("Recalculated!")
            
        stock = get_data("Master_Stock")
        if not stock.empty:
            stock = stock[stock['kms_year'] == kms_year]
            
            # Editable Grid for "Issued Milling"
            edited_df = st.data_editor(stock[['date', 'received_today', 'issued_milling', 'closing_balance']], key="stock_edit")
            
            if st.button("Save Stock Changes"):
                # This is tricky in Sheets. For now, simple logic:
                # We iterate the edited DF and update the sheet.
                # NOTE: This is a basic implementation.
                sh = get_gsheet_connection()
                ws = sh.worksheet("Master_Stock")
                
                # We need to find the matching row for each date and update Col 7 (Issued)
                # This is slow, but safe.
                all_records = ws.get_all_records()
                for i, record in enumerate(all_records):
                    # Find matching date in edited_df
                    row_date = record['date']
                    match = edited_df[edited_df['date'] == row_date]
                    if not match.empty:
                        new_issued = match.iloc[0]['issued_milling']
                        # Update cell (Row = i+2, Col = 7)
                        ws.update_cell(i+2, 7, new_issued)
                
                update_stock_logic(kms_year) # Recalc totals after update
                st.success("Updated!")

    # 3. VEHICLES
    with tabs[2]:
        new_v = st.text_input("New Vehicle No")
        new_o = st.text_input("Owner")
        if st.button("Add Vehicle"):
            sh = get_gsheet_connection()
            ws = sh.worksheet("Vehicle_Registry")
            ws.append_row([new_v, new_o, "", "", 1])
            st.success("Added!")
            
        v_data = get_data("Vehicle_Registry")
        st.dataframe(v_data)

    # 4. SETTINGS
    with tabs[3]:
        st.write("Edit Mandis/Godowns directly in Google Sheets for now.")
        st.markdown(f"[Open Google Sheet](https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID)")

# --- MAIN ---
if __name__ == "__main__":
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        
    if st.session_state['logged_in']:
        show_dashboard()
    else:
        show_login()

