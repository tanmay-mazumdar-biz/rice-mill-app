"""
Rice Mill Procurement System v2.0
With Google Sheets Integration for Permanent Data Storage
"""

import streamlit as st
import pandas as pd
import hashlib
from datetime import datetime, date, timedelta
import gspread
from google.oauth2.service_account import Credentials

# Page configuration
st.set_page_config(
    page_title="Rice Mill Procurement System",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
WEIGHT_PER_BAG = 0.51  # Quintals per bag (51 kg)
DIFFERENCE_THRESHOLD = 2.0  # Quintals - flag if difference exceeds this
SHEET_ID = "1GzSLPc0v1qyuPdxbW-_wut2LF78_MxltVkbGwh8TvVg"

# Sheet names
SHEET_USERS = "Users"
SHEET_EMPLOYEE_ARRIVALS = "Employee_Arrivals"
SHEET_ADMIN_ARRIVALS = "Admin_Arrivals"
SHEET_MASTER_STOCK = "Master_Stock"
SHEET_MANDIS = "Mandis"
SHEET_GODOWNS = "Godowns"
SHEET_VEHICLES = "Vehicles"

def get_kms_year_from_date(entry_date):
    """Auto-detect KMS year from date. KMS year runs Oct to Sep."""
    if isinstance(entry_date, str):
        entry_date = datetime.strptime(entry_date, "%Y-%m-%d").date()
    
    year = entry_date.year
    month = entry_date.month
    
    if month >= 10:
        kms_start_year = year
    else:
        kms_start_year = year - 1
    
    kms_end_year = kms_start_year + 1
    return f"{kms_start_year}-{str(kms_end_year)[-2:]}"

def get_current_kms_year():
    """Get KMS year for current date"""
    return get_kms_year_from_date(date.today())

# ============== GOOGLE SHEETS CONNECTION ==============

@st.cache_resource
def get_gsheet_connection():
    """Create Google Sheets connection"""
    try:
        credentials = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        return None

def get_spreadsheet():
    """Get the spreadsheet object"""
    client = get_gsheet_connection()
    if client:
        return client.open_by_key(SHEET_ID)
    return None

def get_or_create_worksheet(sheet_name, headers):
    """Get worksheet or create if not exists"""
    spreadsheet = get_spreadsheet()
    if not spreadsheet:
        return None
    
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
        worksheet.append_row(headers)
    
    return worksheet

def init_sheets():
    """Initialize all required sheets with headers - runs only once"""
    import time
    
    sheets_config = {
        SHEET_USERS: ["id", "username", "password_hash", "role", "full_name", "phone", "is_active", "created_at"],
        SHEET_EMPLOYEE_ARRIVALS: ["id", "date", "kms_year", "mandi_name", "vehicle_number", "bags", 
                                   "weight_quintals", "godown", "expected_weight", "difference", 
                                   "entered_by", "entry_timestamp", "remarks"],
        SHEET_ADMIN_ARRIVALS: ["id", "date", "kms_year", "mandi_name", "vehicle_number", "ac_note",
                               "quantity_quintals", "entered_by", "entry_timestamp", "remarks"],
        SHEET_MASTER_STOCK: ["id", "date", "kms_year", "opening_balance", "received_today", "prog_received",
                             "total", "issued_milling", "prog_milling", "closing_balance", "remarks"],
        SHEET_MANDIS: ["id", "mandi_name", "distance_km"],
        SHEET_GODOWNS: ["id", "godown_name"],
        SHEET_VEHICLES: ["id", "vehicle_number", "owner_name", "puc_expiry_date", "permit_number", "is_active"]
    }
    
    spreadsheet = get_spreadsheet()
    if not spreadsheet:
        return
    
    # Get existing sheet names
    existing_sheets = [ws.title for ws in spreadsheet.worksheets()]
    
    for sheet_name, headers in sheets_config.items():
        if sheet_name not in existing_sheets:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            worksheet.append_row(headers)
            time.sleep(1)  # Rate limiting
    
    # Add default data only if sheets are empty
    time.sleep(1)
    
    # Add default admin if not exists
    users_df = get_all_data(SHEET_USERS)
    if users_df.empty:
        add_row(SHEET_USERS, {
            "id": 1,
            "username": "admin",
            "password_hash": hash_password("admin123"),
            "role": "admin",
            "full_name": "Administrator",
            "phone": "",
            "is_active": "1",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        time.sleep(1)
    
    # Add default mandis if not exists
    mandis_df = get_all_data(SHEET_MANDIS)
    if mandis_df.empty:
        mandis = ['BHEJENGIWADA', 'CHALANGUDA', 'GUMKA', 'KALIMELA', 'M.V-11', 'M.V-26',
                  'MARIWADA', 'MARKAPALLY', 'MATAPAKA', 'PUSUGUDA', 'UDDUPA']
        for i, mandi in enumerate(mandis, 1):
            add_row(SHEET_MANDIS, {"id": i, "mandi_name": mandi, "distance_km": 0})
            time.sleep(0.5)
    
    # Add default godowns if not exists
    godowns_df = get_all_data(SHEET_GODOWNS)
    if godowns_df.empty:
        for i, godown in enumerate(['Hoper', 'G-3', 'S-2'], 1):
            add_row(SHEET_GODOWNS, {"id": i, "godown_name": godown})
            time.sleep(0.5)
    
    # Add default vehicles if not exists
    vehicles_df = get_all_data(SHEET_VEHICLES)
    if vehicles_df.empty:
        vehicles = ['AP31TU1719', 'CG08Z6713', 'CG17KL6229', 'OD30A9549', 'OD30B3879',
                    'OD30B5356', 'OD30H0487', 'OR10C5722', 'OR301611']
        for i, vehicle in enumerate(vehicles, 1):
            add_row(SHEET_VEHICLES, {
                "id": i, "vehicle_number": vehicle, "owner_name": "", 
                "puc_expiry_date": "", "permit_number": "", "is_active": "1"
            })
            time.sleep(0.5)

def get_all_data(sheet_name):
    """Get all data from a sheet as DataFrame - with caching"""
    # Use session state cache to reduce API calls
    cache_key = f"cache_{sheet_name}"
    cache_time_key = f"cache_time_{sheet_name}"
    
    # Check if cached data exists and is less than 30 seconds old
    if cache_key in st.session_state and cache_time_key in st.session_state:
        cache_age = (datetime.now() - st.session_state[cache_time_key]).total_seconds()
        if cache_age < 30:  # Use cache for 30 seconds
            return st.session_state[cache_key]
    
    try:
        spreadsheet = get_spreadsheet()
        if not spreadsheet:
            return pd.DataFrame()
        
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Store in cache
        st.session_state[cache_key] = df
        st.session_state[cache_time_key] = datetime.now()
        
        return df
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error reading {sheet_name}: {e}")
        return pd.DataFrame()

def clear_cache(sheet_name=None):
    """Clear cached data to force fresh read"""
    if sheet_name:
        cache_key = f"cache_{sheet_name}"
        cache_time_key = f"cache_time_{sheet_name}"
        if cache_key in st.session_state:
            del st.session_state[cache_key]
        if cache_time_key in st.session_state:
            del st.session_state[cache_time_key]
    else:
        # Clear all caches
        keys_to_delete = [k for k in st.session_state.keys() if k.startswith("cache_")]
        for k in keys_to_delete:
            del st.session_state[k]

def add_row(sheet_name, data_dict):
    """Add a row to a sheet"""
    try:
        spreadsheet = get_spreadsheet()
        if not spreadsheet:
            return False
        
        worksheet = spreadsheet.worksheet(sheet_name)
        headers = worksheet.row_values(1)
        row = [str(data_dict.get(h, "")) for h in headers]
        worksheet.append_row(row)
        clear_cache(sheet_name)  # Clear cache after adding
        return True
    except Exception as e:
        st.error(f"Error adding row to {sheet_name}: {e}")
        return False

def update_row(sheet_name, row_id, data_dict):
    """Update a row by ID"""
    try:
        spreadsheet = get_spreadsheet()
        if not spreadsheet:
            return False
        
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        
        for i, row in enumerate(data, start=2):  # Start from row 2 (after header)
            if str(row.get('id')) == str(row_id):
                headers = worksheet.row_values(1)
                for j, header in enumerate(headers, start=1):
                    if header in data_dict:
                        worksheet.update_cell(i, j, str(data_dict[header]))
                clear_cache(sheet_name)  # Clear cache after updating
                return True
        return False
    except Exception as e:
        st.error(f"Error updating {sheet_name}: {e}")
        return False

def delete_row(sheet_name, row_id):
    """Delete a row by ID"""
    try:
        spreadsheet = get_spreadsheet()
        if not spreadsheet:
            return False
        
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        
        for i, row in enumerate(data, start=2):
            if str(row.get('id')) == str(row_id):
                worksheet.delete_rows(i)
                clear_cache(sheet_name)  # Clear cache after deleting
                return True
        return False
    except Exception as e:
        st.error(f"Error deleting from {sheet_name}: {e}")
        return False

def get_next_id(sheet_name):
    """Get next available ID for a sheet"""
    df = get_all_data(sheet_name)
    if df.empty or 'id' not in df.columns:
        return 1
    return int(df['id'].max()) + 1

# ============== HELPER FUNCTIONS ==============

def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_login(username, password):
    """Verify user credentials"""
    users_df = get_all_data(SHEET_USERS)
    if users_df.empty:
        return None
    
    user = users_df[(users_df['username'] == username) & 
                    (users_df['password_hash'] == hash_password(password)) &
                    (users_df['is_active'].astype(str) == '1')]
    
    if not user.empty:
        row = user.iloc[0]
        return (row['id'], row['username'], row['role'], row['full_name'])
    return None

def get_employee_arrivals(kms_year, user=None, start_date=None, end_date=None):
    """Fetch employee arrivals with filters"""
    df = get_all_data(SHEET_EMPLOYEE_ARRIVALS)
    if df.empty:
        return df
    
    df = df[df['kms_year'] == kms_year]
    
    if user:
        df = df[df['entered_by'] == user]
    if start_date:
        df = df[df['date'] >= str(start_date)]
    if end_date:
        df = df[df['date'] <= str(end_date)]
    
    return df.sort_values('date', ascending=False) if not df.empty else df

def get_admin_arrivals(kms_year, start_date=None, end_date=None):
    """Fetch admin arrivals with filters"""
    df = get_all_data(SHEET_ADMIN_ARRIVALS)
    if df.empty:
        return df
    
    df = df[df['kms_year'] == kms_year]
    
    if start_date:
        df = df[df['date'] >= str(start_date)]
    if end_date:
        df = df[df['date'] <= str(end_date)]
    
    return df.sort_values('date', ascending=False) if not df.empty else df

def get_master_stock(kms_year):
    """Fetch master stock for KMS year"""
    df = get_all_data(SHEET_MASTER_STOCK)
    if df.empty:
        return df
    
    df = df[df['kms_year'] == kms_year]
    return df.sort_values('date') if not df.empty else df

def update_master_stock(kms_year):
    """Recalculate master stock from admin arrivals"""
    admin_df = get_all_data(SHEET_ADMIN_ARRIVALS)
    if admin_df.empty:
        return
    
    admin_df = admin_df[admin_df['kms_year'] == kms_year]
    if admin_df.empty:
        return
    
    # Group by date
    admin_df['quantity_quintals'] = pd.to_numeric(admin_df['quantity_quintals'], errors='coerce')
    daily = admin_df.groupby('date')['quantity_quintals'].sum().reset_index()
    daily = daily.sort_values('date')
    
    # Get existing stock data to preserve issued_milling
    stock_df = get_all_data(SHEET_MASTER_STOCK)
    existing_issued = {}
    if not stock_df.empty:
        stock_kms = stock_df[stock_df['kms_year'] == kms_year]
        for _, row in stock_kms.iterrows():
            existing_issued[row['date']] = float(row.get('issued_milling', 0) or 0)
    
    # Clear existing stock for this KMS year
    if not stock_df.empty:
        ids_to_delete = stock_df[stock_df['kms_year'] == kms_year]['id'].tolist()
        for rid in ids_to_delete:
            delete_row(SHEET_MASTER_STOCK, rid)
    
    # Recalculate
    prog_received = 0
    prog_milling = 0
    prev_closing = 0
    
    for _, row in daily.iterrows():
        stock_date = row['date']
        received = row['quantity_quintals']
        prog_received += received
        issued = existing_issued.get(stock_date, 0)
        prog_milling += issued
        
        opening = prev_closing
        total = opening + received
        closing = total - issued
        prev_closing = closing
        
        add_row(SHEET_MASTER_STOCK, {
            "id": get_next_id(SHEET_MASTER_STOCK),
            "date": stock_date,
            "kms_year": kms_year,
            "opening_balance": opening,
            "received_today": received,
            "prog_received": prog_received,
            "total": total,
            "issued_milling": issued,
            "prog_milling": prog_milling,
            "closing_balance": closing,
            "remarks": ""
        })

def to_excel(df):
    """Convert DataFrame to Excel bytes"""
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ============== UI COMPONENTS ==============

def show_login_page():
    """Display login page"""
    st.markdown("""
        <div style='text-align: center; padding: 50px;'>
            <h1>🌾 Rice Mill Procurement System</h1>
            <p style='color: #666;'>Please login to continue</p>
        </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("🔐 Login", use_container_width=True)
            
            if submit:
                if username and password:
                    user = verify_login(username, password)
                    if user:
                        st.session_state['logged_in'] = True
                        st.session_state['user_id'] = user[0]
                        st.session_state['username'] = user[1]
                        st.session_state['role'] = user[2]
                        st.session_state['full_name'] = user[3]
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password")
                else:
                    st.warning("⚠️ Please enter username and password")
        
        st.markdown("---")
        st.markdown("""
            <div style='text-align: center; color: #888; font-size: 12px;'>
                <p>Default Admin: admin / admin123</p>
            </div>
        """, unsafe_allow_html=True)

def show_sidebar():
    """Display sidebar"""
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state['full_name']}")
        st.markdown(f"**Role:** {'🔑 Admin' if st.session_state['role'] == 'admin' else '👷 Employee'}")
        
        st.markdown("---")
        
        # KMS Year Selection
        st.subheader("📅 KMS Year")
        current_kms = get_current_kms_year()
        current_start = int(current_kms.split("-")[0])
        year_options = [f"{y}-{str(y+1)[-2:]}" for y in range(current_start-2, current_start+2)]
        default_idx = year_options.index(current_kms) if current_kms in year_options else 0
        kms_year = st.selectbox("Select Year", year_options, index=default_idx, label_visibility="collapsed")
        st.session_state['kms_year'] = kms_year
        st.caption(f"Current KMS: {current_kms}")
        
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

def show_employee_dashboard():
    """Display employee dashboard"""
    st.title("🌾 Rice Mill - Employee Portal")
    
    tab1, tab2, tab3 = st.tabs(["📝 New Entry", "📋 My Entries", "📊 My Summary"])
    
    kms_year = st.session_state['kms_year']
    username = st.session_state['username']
    
    # TAB 1: New Entry
    with tab1:
        st.subheader("Add New Arrival Entry")
        
        col1, col2 = st.columns(2)
        
        with col1:
            entry_date = st.date_input("Date", value=date.today(), max_value=date.today())
            entry_kms_year = get_kms_year_from_date(entry_date)
            st.caption(f"📅 KMS Year: **{entry_kms_year}**")
            
            days_old = (date.today() - entry_date).days
            if days_old > 7:
                st.warning(f"⚠️ Entry is {days_old} days old.")
            
            mandis_df = get_all_data(SHEET_MANDIS)
            if not mandis_df.empty:
                mandi_options = mandis_df['mandi_name'].tolist()
                selected_mandis = st.multiselect("Mandi (select one or more)", mandi_options)
                selected_mandi = " + ".join(selected_mandis) if selected_mandis else None
                if selected_mandi:
                    st.info(f"📍 Selected: **{selected_mandi}**")
            else:
                st.warning("⚠️ No mandis configured.")
                selected_mandi = None
            
            bags = st.number_input("Number of Bags", min_value=0, value=0, step=1)
        
        with col2:
            vehicles_df = get_all_data(SHEET_VEHICLES)
            vehicles_df = vehicles_df[vehicles_df['is_active'].astype(str) == '1'] if not vehicles_df.empty else vehicles_df
            
            if not vehicles_df.empty:
                vehicle_options = vehicles_df['vehicle_number'].tolist()
                selected_vehicle = st.selectbox("Vehicle Number", vehicle_options)
            else:
                st.warning("⚠️ No vehicles registered.")
                selected_vehicle = None
            
            godowns_df = get_all_data(SHEET_GODOWNS)
            if not godowns_df.empty:
                godown_options = godowns_df['godown_name'].tolist()
                selected_godown = st.selectbox("Godown", godown_options)
            else:
                selected_godown = None
            
            weight = st.number_input("Actual Weight (Quintals)", min_value=0.0, value=0.0, step=0.1)
        
        # Auto-calculated fields
        if bags > 0:
            expected_weight = round(bags * WEIGHT_PER_BAG, 2)
            difference = round(weight - expected_weight, 2)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Expected Weight", f"{expected_weight} Q")
            col2.metric("Actual Weight", f"{weight} Q")
            
            if abs(difference) > DIFFERENCE_THRESHOLD:
                col3.metric("Difference", f"{difference:+.2f} Q", delta_color="inverse")
                st.warning(f"⚠️ Difference exceeds ±{DIFFERENCE_THRESHOLD} Q")
            else:
                col3.metric("Difference", f"{difference:+.2f} Q")
        else:
            expected_weight = 0
            difference = 0
        
        remarks = st.text_input("Remarks (Optional)")
        
        st.markdown("---")
        
        if st.button("💾 Submit Entry", type="primary", use_container_width=True):
            if not selected_mandi:
                st.error("❌ Please select a mandi")
            elif not selected_vehicle:
                st.error("❌ Please select a vehicle")
            elif bags <= 0:
                st.error("❌ Bags must be greater than 0")
            elif weight <= 0:
                st.error("❌ Weight must be greater than 0")
            else:
                success = add_row(SHEET_EMPLOYEE_ARRIVALS, {
                    "id": get_next_id(SHEET_EMPLOYEE_ARRIVALS),
                    "date": str(entry_date),
                    "kms_year": entry_kms_year,
                    "mandi_name": selected_mandi,
                    "vehicle_number": selected_vehicle,
                    "bags": bags,
                    "weight_quintals": weight,
                    "godown": selected_godown or "",
                    "expected_weight": expected_weight,
                    "difference": difference,
                    "entered_by": username,
                    "entry_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "remarks": remarks
                })
                
                if success:
                    st.success("✅ Entry saved successfully!")
                    st.balloons()
    
    # TAB 2: My Entries
    with tab2:
        st.subheader("My Entries")
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("From Date", value=date.today() - timedelta(days=30), key="emp_start")
        with col2:
            end_date = st.date_input("To Date", value=date.today(), key="emp_end")
        
        entries_df = get_employee_arrivals(kms_year, username, start_date, end_date)
        
        if not entries_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Entries", len(entries_df))
            col2.metric("Total Bags", f"{entries_df['bags'].astype(int).sum():,}")
            col3.metric("Total Weight", f"{entries_df['weight_quintals'].astype(float).sum():,.2f} Q")
            col4.metric("Avg Diff", f"{entries_df['difference'].astype(float).mean():+.2f} Q")
            
            st.markdown("---")
            st.dataframe(entries_df[['date', 'mandi_name', 'vehicle_number', 'bags', 
                                     'weight_quintals', 'godown', 'difference']], 
                        use_container_width=True, hide_index=True)
            
            st.download_button(
                "📥 Download My Entries",
                to_excel(entries_df),
                f"my_entries_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No entries found.")
    
    # TAB 3: My Summary
    with tab3:
        st.subheader("My Performance Summary")
        
        all_entries = get_employee_arrivals(kms_year, username)
        
        if not all_entries.empty:
            all_entries['bags'] = all_entries['bags'].astype(int)
            all_entries['weight_quintals'] = all_entries['weight_quintals'].astype(float)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### 📊 By Mandi")
                mandi_summary = all_entries.groupby('mandi_name').agg({
                    'bags': 'sum',
                    'weight_quintals': 'sum'
                }).sort_values('weight_quintals', ascending=False)
                st.dataframe(mandi_summary, use_container_width=True)
            
            with col2:
                st.markdown("##### 🚛 By Vehicle")
                vehicle_summary = all_entries.groupby('vehicle_number').agg({
                    'bags': 'sum',
                    'weight_quintals': 'sum'
                }).sort_values('weight_quintals', ascending=False)
                st.dataframe(vehicle_summary, use_container_width=True)
        else:
            st.info("ℹ️ No entries found.")

def show_admin_dashboard():
    """Display admin dashboard"""
    st.title("🌾 Rice Mill - Admin Portal")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📝 Admin Entry", "📊 Master Stock", "🔄 Comparison",
        "👁️ Employee Data", "🚛 Vehicles", "⚙️ Settings"
    ])
    
    kms_year = st.session_state['kms_year']
    username = st.session_state['username']
    
    # TAB 1: Admin Entry
    with tab1:
        st.subheader("Official Arrival Register")
        
        entry_col, list_col = st.columns([1, 2])
        
        with entry_col:
            st.markdown("##### ➕ Add Entry")
            
            entry_date = st.date_input("Date", value=date.today(), key="admin_date")
            entry_kms_year = get_kms_year_from_date(entry_date)
            st.caption(f"📅 KMS Year: **{entry_kms_year}**")
            
            mandis_df = get_all_data(SHEET_MANDIS)
            if not mandis_df.empty:
                mandi_options = mandis_df['mandi_name'].tolist()
                selected_mandis = st.multiselect("Mandi (select one or more)", mandi_options, key="admin_mandi")
                selected_mandi = " + ".join(selected_mandis) if selected_mandis else None
                if selected_mandi:
                    st.info(f"📍 Selected: **{selected_mandi}**")
            else:
                selected_mandi = None
            
            vehicles_df = get_all_data(SHEET_VEHICLES)
            vehicles_df = vehicles_df[vehicles_df['is_active'].astype(str) == '1'] if not vehicles_df.empty else vehicles_df
            
            if not vehicles_df.empty:
                selected_vehicle = st.selectbox("Vehicle/Truck", vehicles_df['vehicle_number'].tolist(), key="admin_vehicle")
            else:
                selected_vehicle = None
            
            ac_note = st.text_input("A/C Note Number")
            quantity = st.number_input("Quantity (Quintals)", min_value=0.0, value=0.0, step=0.1)
            remarks = st.text_input("Remarks", key="admin_remarks")
            
            if st.button("💾 Add Entry", type="primary", use_container_width=True):
                if not selected_mandi:
                    st.error("❌ Please select a mandi")
                elif not selected_vehicle:
                    st.error("❌ Please select a vehicle")
                elif quantity <= 0:
                    st.error("❌ Quantity must be greater than 0")
                else:
                    success = add_row(SHEET_ADMIN_ARRIVALS, {
                        "id": get_next_id(SHEET_ADMIN_ARRIVALS),
                        "date": str(entry_date),
                        "kms_year": entry_kms_year,
                        "mandi_name": selected_mandi,
                        "vehicle_number": selected_vehicle,
                        "ac_note": ac_note,
                        "quantity_quintals": quantity,
                        "entered_by": username,
                        "entry_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "remarks": remarks
                    })
                    
                    if success:
                        update_master_stock(entry_kms_year)
                        st.success("✅ Entry added!")
                        st.rerun()
        
        with list_col:
            st.markdown("##### 📋 Arrival Register")
            
            admin_df = get_admin_arrivals(kms_year)
            
            if not admin_df.empty:
                for date_val in admin_df['date'].unique():
                    day_data = admin_df[admin_df['date'] == date_val]
                    daily_total = day_data['quantity_quintals'].astype(float).sum()
                    
                    st.markdown(f"**📅 {date_val}** — Total: **{daily_total:.2f} Q**")
                    
                    for _, row in day_data.iterrows():
                        cols = st.columns([2, 2, 1, 1, 1])
                        cols[0].write(f"🏪 {row['mandi_name']}")
                        cols[1].write(f"🚛 {row['vehicle_number']}")
                        cols[2].write(f"📝 {row['ac_note'] or '-'}")
                        cols[3].write(f"⚖️ {row['quantity_quintals']} Q")
                        
                        if cols[4].button("🗑️", key=f"del_{row['id']}"):
                            delete_row(SHEET_ADMIN_ARRIVALS, row['id'])
                            update_master_stock(kms_year)
                            st.rerun()
                    
                    st.divider()
                
                total_qty = admin_df['quantity_quintals'].astype(float).sum()
                st.markdown(f"### 📊 Total: {total_qty:.2f} Q")
                
                st.download_button(
                    "📥 Download Register",
                    to_excel(admin_df[['date', 'mandi_name', 'vehicle_number', 'ac_note', 'quantity_quintals']]),
                    f"admin_register_{kms_year}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("ℹ️ No entries yet.")
    
    # TAB 2: Master Stock
    with tab2:
        st.subheader("Master Stock Register")
        
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔄 Recalculate", use_container_width=True):
                update_master_stock(kms_year)
                st.success("✅ Recalculated!")
                st.rerun()
        
        stock_df = get_master_stock(kms_year)
        
        if not stock_df.empty:
            display_df = stock_df[['date', 'opening_balance', 'received_today', 'prog_received',
                                   'total', 'issued_milling', 'prog_milling', 'closing_balance']].copy()
            display_df.columns = ['Date', 'O/B', 'Received', 'Prog.Recv', 'Total', 
                                 'Issue Mill', 'Prog.Mill', 'C/B']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Received", f"{stock_df['received_today'].astype(float).sum():.2f} Q")
            col2.metric("Total Issued", f"{stock_df['issued_milling'].astype(float).sum():.2f} Q")
            col3.metric("Current Stock", f"{stock_df['closing_balance'].astype(float).iloc[-1]:.2f} Q")
            col4.metric("Prog. Received", f"{stock_df['prog_received'].astype(float).iloc[-1]:.2f} Q")
            
            st.download_button(
                "📥 Download Stock Register",
                to_excel(display_df),
                f"master_stock_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No stock data. Add arrivals first.")
    
    # TAB 3: Comparison
    with tab3:
        st.subheader("Employee vs Admin Comparison")
        
        col1, col2 = st.columns(2)
        with col1:
            comp_start = st.date_input("From", value=date.today() - timedelta(days=30), key="comp_start")
        with col2:
            comp_end = st.date_input("To", value=date.today(), key="comp_end")
        
        emp_df = get_employee_arrivals(kms_year, start_date=comp_start, end_date=comp_end)
        adm_df = get_admin_arrivals(kms_year, start_date=comp_start, end_date=comp_end)
        
        if not emp_df.empty or not adm_df.empty:
            emp_daily = emp_df.groupby('date')['weight_quintals'].apply(lambda x: x.astype(float).sum()) if not emp_df.empty else pd.Series()
            adm_daily = adm_df.groupby('date')['quantity_quintals'].apply(lambda x: x.astype(float).sum()) if not adm_df.empty else pd.Series()
            
            all_dates = sorted(set(emp_daily.index) | set(adm_daily.index))
            
            comparison_data = []
            for d in all_dates:
                emp_val = emp_daily.get(d, 0)
                adm_val = adm_daily.get(d, 0)
                diff = emp_val - adm_val
                
                if emp_val == 0:
                    status = "❌ Missing Employee"
                elif adm_val == 0:
                    status = "❌ Missing Admin"
                elif abs(diff) < 1:
                    status = "✅ Match"
                elif abs(diff) < 50:
                    status = "⚠️ Minor Diff"
                else:
                    status = "❌ Major Diff"
                
                comparison_data.append({
                    'Date': d, 'Employee (Q)': emp_val, 'Admin (Q)': adm_val,
                    'Difference': diff, 'Status': status
                })
            
            comp_df = pd.DataFrame(comparison_data)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Employee Total", f"{emp_daily.sum():.2f} Q")
            col2.metric("Admin Total", f"{adm_daily.sum():.2f} Q")
            col3.metric("Difference", f"{emp_daily.sum() - adm_daily.sum():+.2f} Q")
            
            st.dataframe(comp_df, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ No data for comparison.")
    
    # TAB 4: Employee Data
    with tab4:
        st.subheader("Employee Entries")
        
        emp_entries = get_employee_arrivals(kms_year)
        
        if not emp_entries.empty:
            st.dataframe(emp_entries[['date', 'mandi_name', 'vehicle_number', 'bags',
                                      'weight_quintals', 'godown', 'difference', 'entered_by']],
                        use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ No employee entries.")
    
    # TAB 5: Vehicles
    with tab5:
        st.subheader("Vehicle Registry")
        
        vcol1, vcol2 = st.columns([1, 2])
        
        with vcol1:
            st.markdown("##### ➕ Add Vehicle")
            new_vehicle = st.text_input("Vehicle Number")
            new_owner = st.text_input("Owner Name")
            
            if st.button("➕ Add Vehicle", type="primary", use_container_width=True):
                if new_vehicle:
                    success = add_row(SHEET_VEHICLES, {
                        "id": get_next_id(SHEET_VEHICLES),
                        "vehicle_number": new_vehicle.strip().upper(),
                        "owner_name": new_owner,
                        "puc_expiry_date": "",
                        "permit_number": "",
                        "is_active": "1"
                    })
                    if success:
                        st.success("✅ Added!")
                        st.rerun()
        
        with vcol2:
            vehicles_df = get_all_data(SHEET_VEHICLES)
            if not vehicles_df.empty:
                st.dataframe(vehicles_df[['vehicle_number', 'owner_name', 'is_active']], 
                            use_container_width=True, hide_index=True)
    
    # TAB 6: Settings
    with tab6:
        settings_tab1, settings_tab2, settings_tab3 = st.tabs(["🏪 Mandis", "🏭 Godowns", "👥 Users"])
        
        with settings_tab1:
            col1, col2 = st.columns(2)
            with col1:
                new_mandi = st.text_input("New Mandi Name")
                if st.button("➕ Add Mandi", type="primary"):
                    if new_mandi:
                        add_row(SHEET_MANDIS, {
                            "id": get_next_id(SHEET_MANDIS),
                            "mandi_name": new_mandi.strip().upper(),
                            "distance_km": 0
                        })
                        st.success("✅ Added!")
                        st.rerun()
            with col2:
                mandis_df = get_all_data(SHEET_MANDIS)
                if not mandis_df.empty:
                    st.dataframe(mandis_df[['mandi_name']], use_container_width=True, hide_index=True)
        
        with settings_tab2:
            col1, col2 = st.columns(2)
            with col1:
                new_godown = st.text_input("New Godown Name")
                if st.button("➕ Add Godown", type="primary"):
                    if new_godown:
                        add_row(SHEET_GODOWNS, {
                            "id": get_next_id(SHEET_GODOWNS),
                            "godown_name": new_godown.strip()
                        })
                        st.success("✅ Added!")
                        st.rerun()
            with col2:
                godowns_df = get_all_data(SHEET_GODOWNS)
                if not godowns_df.empty:
                    st.dataframe(godowns_df[['godown_name']], use_container_width=True, hide_index=True)
        
        with settings_tab3:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Add New User**")
                new_username = st.text_input("Username")
                new_password = st.text_input("Password", type="password")
                new_fullname = st.text_input("Full Name")
                new_role = st.selectbox("Role", ["employee", "admin"])
                
                if st.button("➕ Add User", type="primary"):
                    if new_username and new_password:
                        add_row(SHEET_USERS, {
                            "id": get_next_id(SHEET_USERS),
                            "username": new_username,
                            "password_hash": hash_password(new_password),
                            "role": new_role,
                            "full_name": new_fullname,
                            "phone": "",
                            "is_active": "1",
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        st.success("✅ User created!")
                        st.rerun()
            with col2:
                users_df = get_all_data(SHEET_USERS)
                if not users_df.empty:
                    st.dataframe(users_df[['username', 'role', 'full_name', 'is_active']], 
                                use_container_width=True, hide_index=True)

# ============== MAIN ==============

def main():
    # Initialize sheets on first run
    if 'initialized' not in st.session_state:
        with st.spinner("Connecting to database..."):
            init_sheets()
        st.session_state['initialized'] = True
    
    if 'logged_in' not in st.session_state or not st.session_state['logged_in']:
        show_login_page()
    else:
        show_sidebar()
        if st.session_state['role'] == 'admin':
            show_admin_dashboard()
        else:
            show_employee_dashboard()

if __name__ == "__main__":
    main()
