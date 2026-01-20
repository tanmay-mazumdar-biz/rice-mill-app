"""
Shiva Shankar Modern Rice Mill - Procurement System v2.0
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
    page_title="Shiva Shankar Modern Rice Mill",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional UI
st.markdown("""
<style>
    /* Main theme colors */
    :root {
        --primary-green: #2E7D32;
        --primary-gold: #F9A825;
        --light-green: #E8F5E9;
        --dark-text: #1B5E20;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #2E7D32 0%, #1B5E20 100%);
        padding: 1.5rem 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .main-header h1 {
        color: white;
        margin: 0;
        font-size: 1.8rem;
        font-weight: 600;
    }
    
    .main-header p {
        color: #C8E6C9;
        margin: 0.5rem 0 0 0;
        font-size: 1rem;
    }
    
    /* Login card styling */
    .login-container {
        max-width: 400px;
        margin: 2rem auto;
        padding: 2rem;
        background: white;
        border-radius: 15px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.1);
    }
    
    .login-header {
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .login-header h2 {
        color: #2E7D32;
        margin-bottom: 0.5rem;
    }
    
    .login-logo {
        font-size: 4rem;
        margin-bottom: 1rem;
    }
    
    /* Metric cards styling */
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f5f5f5 100%);
        padding: 1.2rem;
        border-radius: 10px;
        border-left: 4px solid #2E7D32;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 1rem;
    }
    
    .metric-card-gold {
        border-left-color: #F9A825;
    }
    
    .metric-card-blue {
        border-left-color: #1976D2;
    }
    
    .metric-card-red {
        border-left-color: #D32F2F;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1B5E20 0%, #2E7D32 100%);
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        color: white;
    }
    
    [data-testid="stSidebar"] .stSelectbox label {
        color: white !important;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(135deg, #2E7D32 0%, #1B5E20 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #1B5E20 0%, #0D3D12 100%);
        box-shadow: 0 4px 12px rgba(46,125,50,0.4);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #1B5E20;
        padding: 0.5rem;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #E8F5E9;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        color: #1B5E20 !important;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #F9A825 !important;
        color: #1B5E20 !important;
        font-weight: 600 !important;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #C8E6C9;
    }
    
    /* Form styling */
    .stTextInput > div > div > input {
        border-radius: 8px;
        border: 2px solid #E0E0E0;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #2E7D32;
        box-shadow: 0 0 0 2px rgba(46,125,50,0.2);
    }
    
    /* Dataframe styling */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }
    
    /* Success/Error messages */
    .stSuccess {
        background-color: #E8F5E9;
        border-left: 4px solid #2E7D32;
        border-radius: 8px;
    }
    
    .stError {
        border-radius: 8px;
    }
    
    /* Section headers */
    .section-header {
        background: #E8F5E9;
        padding: 0.8rem 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        border-left: 4px solid #2E7D32;
    }
    
    /* Divider */
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, #2E7D32, #F9A825, #2E7D32);
        margin: 1.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Constants
WEIGHT_PER_BAG = 0.51  # Quintals per bag (51 kg)
DIFFERENCE_THRESHOLD = 2.0  # Quintals - flag if difference exceeds this
SHEET_ID = "1GzSLPc0v1qyuPdxbW-_wut2LF78_MxltVkbGwh8TvVg"

# Sheet names
SHEET_USERS = "Users"
SHEET_EMPLOYEE_ARRIVALS = "Employee_Arrivals"
SHEET_ADMIN_ARRIVALS = "Admin_Arrivals"
SHEET_MASTER_STOCK = "Master_Stock"
SHEET_MILLING = "Milling"
SHEET_DIESEL = "Diesel"
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
    except KeyError:
        st.error("❌ Missing secrets! Add gcp_service_account to Streamlit secrets.")
        return None
    except Exception as e:
        st.error(f"❌ Failed to connect to Google Sheets: {e}")
        return None

def get_spreadsheet():
    """Get the spreadsheet object"""
    client = get_gsheet_connection()
    if client:
        try:
            spreadsheet = client.open_by_key(SHEET_ID)
            return spreadsheet
        except gspread.SpreadsheetNotFound:
            st.error(f"❌ Spreadsheet not found! Check SHEET_ID: {SHEET_ID}")
            return None
        except gspread.exceptions.APIError as e:
            st.error(f"❌ API Error: {e}")
            return None
        except Exception as e:
            st.error(f"❌ Error opening spreadsheet: {e}")
            return None
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
        SHEET_MILLING: ["id", "date", "kms_year", "issued_quintals", "remarks", "entered_by", "entry_timestamp"],
        SHEET_DIESEL: ["id", "date", "kms_year", "vehicle_number", "liters", "amount", "pump_station", "entered_by", "entry_timestamp"],
        SHEET_MANDIS: ["id", "mandi_name", "distance_km"],
        SHEET_GODOWNS: ["id", "godown_name"],
        SHEET_VEHICLES: ["id", "vehicle_number", "owner_name", "puc_expiry_date", "permit_number", "is_active"]
    }
    
    spreadsheet = get_spreadsheet()
    if not spreadsheet:
        return
    
    # Get existing sheet names
    try:
        existing_sheets = [ws.title for ws in spreadsheet.worksheets()]
    except Exception as e:
        return
    
    for sheet_name, headers in sheets_config.items():
        if sheet_name not in existing_sheets:
            try:
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                worksheet.append_row(headers)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                pass  # Silently handle errors
    
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

def to_excel(df):
    """Convert DataFrame to Excel bytes"""
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ============== UI COMPONENTS ==============

def show_login_page():
    """Display login page with professional branding"""
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 1.5, 1])
    
    with col2:
        # Logo and branding
        st.markdown("""
            <div style='text-align: center; padding: 2rem 0;'>
                <div style='font-size: 5rem; margin-bottom: 1rem;'>🌾</div>
                <h1 style='color: #2E7D32; margin: 0; font-size: 1.8rem; font-weight: 700;'>
                    Shiva Shankar
                </h1>
                <h2 style='color: #1B5E20; margin: 0.2rem 0 1rem 0; font-size: 1.3rem; font-weight: 500;'>
                    Modern Rice Mill
                </h2>
                <p style='color: #666; font-size: 0.95rem;'>Procurement Management System</p>
            </div>
        """, unsafe_allow_html=True)
        
        # Login form with styled container
        st.markdown("""
            <div style='background: linear-gradient(135deg, #E8F5E9 0%, #C8E6C9 100%); 
                        padding: 0.5rem; border-radius: 15px; margin: 1rem 0;'>
            </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            st.markdown("#### 🔐 Login")
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            submit = st.form_submit_button("Login", use_container_width=True)
            
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
        
        # Footer
        st.markdown("""
            <div style='text-align: center; margin-top: 2rem; padding: 1rem; 
                        background: #f5f5f5; border-radius: 10px;'>
                <p style='color: #888; font-size: 0.8rem; margin: 0;'>
                    📍 Powered by Streamlit | © 2026 Shiva Shankar Modern Rice Mill
                </p>
            </div>
        """, unsafe_allow_html=True)

def show_sidebar():
    """Display sidebar with branding"""
    with st.sidebar:
        # Sidebar header with branding
        st.markdown("""
            <div style='text-align: center; padding: 1rem 0; margin-bottom: 1rem;'>
                <div style='font-size: 2.5rem;'>🌾</div>
                <p style='color: white; font-weight: 600; margin: 0.5rem 0 0 0; font-size: 1.1rem;'>SSMRM</p>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # User info
        st.markdown(f"### 👤 {st.session_state['full_name']}")
        role_badge = "🔑 Admin" if st.session_state['role'] == 'admin' else "👷 Employee"
        st.markdown(f"**Role:** {role_badge}")
        
        st.markdown("---")
        
        # KMS Year Selection
        st.markdown("##### 📅 KMS Year")
        current_kms = get_current_kms_year()
        current_start = int(current_kms.split("-")[0])
        year_options = [f"{y}-{str(y+1)[-2:]}" for y in range(current_start-2, current_start+2)]
        default_idx = year_options.index(current_kms) if current_kms in year_options else 0
        kms_year = st.selectbox("Select Year", year_options, index=default_idx, label_visibility="collapsed")
        st.session_state['kms_year'] = kms_year
        st.caption(f"📆 Current: {current_kms}")
        
        st.markdown("---")
        
        # Logout button
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        
        # Sidebar footer
        st.markdown("---")
        st.markdown("""
            <div style='text-align: center; padding: 0.5rem;'>
                <p style='color: rgba(255,255,255,0.7); font-size: 0.7rem; margin: 0;'>
                    v2.0 | 2026
                </p>
            </div>
        """, unsafe_allow_html=True)

def show_employee_dashboard():
    """Display employee dashboard"""
    
    # Professional header
    st.markdown("""
        <div class='main-header'>
            <h1>🌾 Shiva Shankar Modern Rice Mill</h1>
            <p>Employee Portal - Paddy Procurement System</p>
        </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📝 New Entry", "📋 My Entries", "📊 My Summary"])
    
    kms_year = st.session_state['kms_year']
    username = st.session_state['username']
    
    # TAB 1: New Entry
    with tab1:
        st.subheader("Add New Arrival Entry")
        
        col1, col2 = st.columns(2)
        
        with col1:
            entry_date = st.date_input("Date", value=date.today(), max_value=date.today(), key="emp_entry_date")
            entry_kms_year = get_kms_year_from_date(entry_date)
            st.caption(f"📅 KMS Year: **{entry_kms_year}**")
            
            days_old = (date.today() - entry_date).days
            if days_old > 7:
                st.warning(f"⚠️ Entry is {days_old} days old.")
            
            mandis_df = get_all_data(SHEET_MANDIS)
            if not mandis_df.empty:
                mandi_options = mandis_df['mandi_name'].tolist()
                # Use expander for mandi selection
                with st.expander("🏪 Select Mandi (click to expand)", expanded=False):
                    selected_mandis = st.multiselect("Choose one or more", mandi_options, key="emp_mandi", label_visibility="collapsed")
                selected_mandi = " + ".join(selected_mandis) if selected_mandis else None
                if selected_mandi:
                    st.success(f"📍 **{selected_mandi}**")
            else:
                st.warning("⚠️ No mandis configured.")
                selected_mandi = None
            
            bags = st.number_input("Number of Bags", min_value=0, value=0, step=1, key="emp_bags")
        
        with col2:
            vehicles_df = get_all_data(SHEET_VEHICLES)
            vehicles_df = vehicles_df[vehicles_df['is_active'].astype(str) == '1'] if not vehicles_df.empty else vehicles_df
            
            if not vehicles_df.empty:
                vehicle_options = [""] + vehicles_df['vehicle_number'].tolist()
                selected_vehicle = st.selectbox("Vehicle Number", vehicle_options, key="emp_vehicle")
                selected_vehicle = selected_vehicle if selected_vehicle else None
            else:
                st.warning("⚠️ No vehicles registered.")
                selected_vehicle = None
            
            godowns_df = get_all_data(SHEET_GODOWNS)
            if not godowns_df.empty:
                godown_options = [""] + godowns_df['godown_name'].tolist()
                selected_godown = st.selectbox("Godown", godown_options, key="emp_godown")
                selected_godown = selected_godown if selected_godown else None
            else:
                selected_godown = None
            
            weight = st.number_input("Actual Weight (Quintals)", min_value=0.0, value=0.0, step=0.1, key="emp_weight")
        
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
        
        remarks = st.text_input("Remarks (Optional)", key="emp_remarks")
        
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
                    # Clear form fields
                    for key in ['emp_mandi', 'emp_vehicle', 'emp_godown', 'emp_bags', 'emp_weight', 'emp_remarks']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
    
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
            
            # Edit mode check
            if 'edit_emp_id' not in st.session_state:
                st.session_state['edit_emp_id'] = None
            
            # Get dropdown options for edit forms
            mandis_list = get_all_data(SHEET_MANDIS)['mandi_name'].tolist() if not get_all_data(SHEET_MANDIS).empty else []
            vehicles_df = get_all_data(SHEET_VEHICLES)
            vehicles_list = vehicles_df[vehicles_df['is_active'].astype(str) == '1']['vehicle_number'].tolist() if not vehicles_df.empty else []
            godowns_list = get_all_data(SHEET_GODOWNS)['godown_name'].tolist() if not get_all_data(SHEET_GODOWNS).empty else []
            
            for _, row in entries_df.iterrows():
                row_id = row['id']
                
                # Check if this row is being edited
                if st.session_state['edit_emp_id'] == row_id:
                    # Edit form with dropdowns
                    with st.form(key=f"edit_emp_form_{row_id}"):
                        st.markdown(f"**Editing entry from {row['date']}**")
                        edit_cols = st.columns([2, 2, 1, 1, 1])
                        
                        # Mandi dropdown
                        current_mandi = row['mandi_name']
                        mandi_idx = mandis_list.index(current_mandi) if current_mandi in mandis_list else 0
                        edit_mandi = edit_cols[0].selectbox("Mandi", mandis_list, index=mandi_idx, key=f"edit_emp_mandi_{row_id}")
                        
                        # Vehicle dropdown
                        current_vehicle = row['vehicle_number']
                        vehicle_idx = vehicles_list.index(current_vehicle) if current_vehicle in vehicles_list else 0
                        edit_vehicle = edit_cols[1].selectbox("Vehicle", vehicles_list, index=vehicle_idx, key=f"edit_emp_vehicle_{row_id}")
                        
                        edit_bags = edit_cols[2].number_input("Bags", value=int(row['bags']), key=f"edit_emp_bags_{row_id}")
                        edit_weight = edit_cols[3].number_input("Weight", value=float(row['weight_quintals']), key=f"edit_emp_weight_{row_id}")
                        
                        # Godown dropdown
                        current_godown = row['godown'] or ''
                        godown_options = [''] + godowns_list
                        godown_idx = godown_options.index(current_godown) if current_godown in godown_options else 0
                        edit_godown = edit_cols[4].selectbox("Godown", godown_options, index=godown_idx, key=f"edit_emp_godown_{row_id}")
                        
                        # Recalculate expected and difference
                        new_expected = round(edit_bags * WEIGHT_PER_BAG, 2)
                        new_diff = round(edit_weight - new_expected, 2)
                        
                        btn_cols = st.columns([1, 1, 2])
                        if btn_cols[0].form_submit_button("💾 Save"):
                            update_row(SHEET_EMPLOYEE_ARRIVALS, row_id, {
                                "mandi_name": edit_mandi,
                                "vehicle_number": edit_vehicle,
                                "bags": edit_bags,
                                "weight_quintals": edit_weight,
                                "godown": edit_godown,
                                "expected_weight": new_expected,
                                "difference": new_diff
                            })
                            st.session_state['edit_emp_id'] = None
                            st.rerun()
                        if btn_cols[1].form_submit_button("❌ Cancel"):
                            st.session_state['edit_emp_id'] = None
                            st.rerun()
                else:
                    # Normal display
                    cols = st.columns([1.5, 2, 2, 1, 1, 1, 1, 0.5, 0.5])
                    cols[0].write(f"📅 {row['date']}")
                    cols[1].write(f"🏪 {row['mandi_name']}")
                    cols[2].write(f"🚛 {row['vehicle_number']}")
                    cols[3].write(f"📦 {row['bags']}")
                    cols[4].write(f"⚖️ {row['weight_quintals']} Q")
                    cols[5].write(f"🏭 {row['godown'] or '-'}")
                    
                    diff = float(row['difference'])
                    if abs(diff) > DIFFERENCE_THRESHOLD:
                        cols[6].write(f"⚠️ {diff:+.2f}")
                    else:
                        cols[6].write(f"✅ {diff:+.2f}")
                    
                    if cols[7].button("✏️", key=f"edit_emp_{row_id}"):
                        st.session_state['edit_emp_id'] = row_id
                        st.rerun()
                    
                    if cols[8].button("🗑️", key=f"del_emp_{row_id}"):
                        delete_row(SHEET_EMPLOYEE_ARRIVALS, row_id)
                        st.rerun()
            
            st.markdown("---")
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
    
    # Professional header
    st.markdown("""
        <div class='main-header'>
            <h1>🌾 Shiva Shankar Modern Rice Mill</h1>
            <p>Admin Portal - Procurement Management System</p>
        </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "🏠 Dashboard", "📝 Admin Entry", "📊 Master Stock", "🏭 Milling", "⛽ Diesel",
        "👁️ Employee Data", "🚛 Vehicles", "⚙️ Settings"
    ])
    
    kms_year = st.session_state['kms_year']
    username = st.session_state['username']
    
    # TAB 1: Dashboard (merged with Comparison)
    with tab1:
        st.subheader("📊 Dashboard & Comparison")
        
        # Get today's data
        today = date.today()
        today_str = str(today)
        
        # Get all data for dashboard
        admin_df_all = get_admin_arrivals(kms_year)
        emp_df_all = get_employee_arrivals(kms_year)
        milling_df = get_all_data(SHEET_MILLING)
        
        # Calculate totals
        total_received = admin_df_all['quantity_quintals'].astype(float).sum() if not admin_df_all.empty else 0
        
        total_milling = 0
        if not milling_df.empty:
            milling_kms = milling_df[milling_df['kms_year'] == kms_year]
            if not milling_kms.empty:
                total_milling = milling_kms['issued_quintals'].astype(float).sum()
        
        current_stock = total_received - total_milling
        
        # Today's entries
        today_admin = admin_df_all[admin_df_all['date'] == today_str] if not admin_df_all.empty else pd.DataFrame()
        today_emp = emp_df_all[emp_df_all['date'] == today_str] if not emp_df_all.empty else pd.DataFrame()
        
        # Summary cards
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("📦 Total Received", f"{total_received:,.2f} Q")
        col2.metric("🏭 Total Milled", f"{total_milling:,.2f} Q")
        col3.metric("📊 Current Stock", f"{current_stock:,.2f} Q")
        col4.metric("📅 Today's Entries", f"{len(today_admin) + len(today_emp)}")
        
        st.markdown("---")
        
        # Vehicle Trip Count (This Month)
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🚛 Vehicle Trips (This Month)")
            current_month = today.strftime("%Y-%m")
            if not admin_df_all.empty:
                admin_df_all['month'] = admin_df_all['date'].str[:7]
                month_trips = admin_df_all[admin_df_all['month'] == current_month]
                if not month_trips.empty:
                    trip_count = month_trips.groupby('vehicle_number').size().reset_index(name='Trips')
                    trip_count = trip_count.sort_values('Trips', ascending=False)
                    st.dataframe(trip_count, use_container_width=True, hide_index=True)
                else:
                    st.info("No trips this month")
            else:
                st.info("No data available")
        
        with col2:
            st.markdown("##### 📈 Recent Activity")
            if not admin_df_all.empty:
                recent = admin_df_all.head(10)[['date', 'mandi_name', 'vehicle_number', 'quantity_quintals']]
                recent.columns = ['Date', 'Mandi', 'Vehicle', 'Qty (Q)']
                st.dataframe(recent, use_container_width=True, hide_index=True)
            else:
                st.info("No recent activity")
        
        st.markdown("---")
        
        # ========== COMPARISON SECTION ==========
        st.markdown("### 🔄 Employee vs Admin Comparison")
        st.caption("📊 Filter by date, mandi, or vehicle to compare data")
        
        # Filter row
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
        
        with filter_col1:
            comp_start = st.date_input("From Date", value=date.today() - timedelta(days=30), key="comp_start")
        with filter_col2:
            comp_end = st.date_input("To Date", value=date.today(), key="comp_end")
        
        # Get mandi and vehicle options for filters
        mandis_df = get_all_data(SHEET_MANDIS)
        vehicles_df = get_all_data(SHEET_VEHICLES)
        
        mandi_options = ["All"] + (mandis_df['mandi_name'].tolist() if not mandis_df.empty else [])
        vehicle_options = ["All"] + (vehicles_df['vehicle_number'].tolist() if not vehicles_df.empty else [])
        
        with filter_col3:
            filter_mandi = st.selectbox("🏪 Mandi", mandi_options, key="comp_mandi")
        with filter_col4:
            filter_vehicle = st.selectbox("🚛 Vehicle", vehicle_options, key="comp_vehicle")
        
        # Get filtered data
        emp_df = get_employee_arrivals(kms_year, start_date=comp_start, end_date=comp_end)
        adm_df = get_admin_arrivals(kms_year, start_date=comp_start, end_date=comp_end)
        
        # Apply mandi filter
        if filter_mandi != "All":
            emp_df = emp_df[emp_df['mandi_name'].str.contains(filter_mandi, na=False)] if not emp_df.empty else emp_df
            adm_df = adm_df[adm_df['mandi_name'].str.contains(filter_mandi, na=False)] if not adm_df.empty else adm_df
        
        # Apply vehicle filter
        if filter_vehicle != "All":
            emp_df = emp_df[emp_df['vehicle_number'] == filter_vehicle] if not emp_df.empty else emp_df
            adm_df = adm_df[adm_df['vehicle_number'] == filter_vehicle] if not adm_df.empty else adm_df
        
        # Overall Totals
        emp_total = emp_df['weight_quintals'].astype(float).sum() if not emp_df.empty else 0
        adm_total = adm_df['quantity_quintals'].astype(float).sum() if not adm_df.empty else 0
        
        col1, col2 = st.columns(2)
        col1.metric("👷 Employee Total", f"{emp_total:,.2f} Q")
        col2.metric("🔑 Admin Total", f"{adm_total:,.2f} Q")
        
        st.markdown("---")
        
        # Detailed filtered data table
        st.markdown("##### 📋 Filtered Data (Date + Mandi + Vehicle)")
        
        # Create combined view
        left_col, right_col = st.columns(2)
        
        with left_col:
            st.markdown("**👷 Employee Entries**")
            if not emp_df.empty:
                emp_display = emp_df[['date', 'mandi_name', 'vehicle_number', 'weight_quintals']].copy()
                emp_display.columns = ['Date', 'Mandi', 'Vehicle', 'Qty (Q)']
                emp_display = emp_display.sort_values('Date', ascending=False)
                st.dataframe(emp_display, use_container_width=True, hide_index=True, height=300)
            else:
                st.info("No employee data")
        
        with right_col:
            st.markdown("**🔑 Admin Entries**")
            if not adm_df.empty:
                adm_display = adm_df[['date', 'mandi_name', 'vehicle_number', 'quantity_quintals']].copy()
                adm_display.columns = ['Date', 'Mandi', 'Vehicle', 'Qty (Q)']
                adm_display = adm_display.sort_values('Date', ascending=False)
                st.dataframe(adm_display, use_container_width=True, hide_index=True, height=300)
            else:
                st.info("No admin data")
        
        st.markdown("---")
        
        # Summary by Mandi and Vehicle
        st.markdown("##### 📊 Summary Breakdown")
        
        sum_col1, sum_col2 = st.columns(2)
        
        with sum_col1:
            st.markdown("**By Mandi**")
            if not emp_df.empty or not adm_df.empty:
                # Get all mandis from both
                all_mandis = set()
                if not emp_df.empty:
                    all_mandis.update(emp_df['mandi_name'].unique())
                if not adm_df.empty:
                    all_mandis.update(adm_df['mandi_name'].unique())
                
                mandi_comparison = []
                for mandi in sorted(all_mandis):
                    emp_qty = emp_df[emp_df['mandi_name'] == mandi]['weight_quintals'].astype(float).sum() if not emp_df.empty else 0
                    adm_qty = adm_df[adm_df['mandi_name'] == mandi]['quantity_quintals'].astype(float).sum() if not adm_df.empty else 0
                    mandi_comparison.append({
                        'Mandi': mandi,
                        'Employee (Q)': round(emp_qty, 2),
                        'Admin (Q)': round(adm_qty, 2)
                    })
                
                if mandi_comparison:
                    st.dataframe(pd.DataFrame(mandi_comparison), use_container_width=True, hide_index=True)
            else:
                st.info("No data")
        
        with sum_col2:
            st.markdown("**By Vehicle**")
            if not emp_df.empty or not adm_df.empty:
                # Get all vehicles from both
                all_vehicles = set()
                if not emp_df.empty:
                    all_vehicles.update(emp_df['vehicle_number'].unique())
                if not adm_df.empty:
                    all_vehicles.update(adm_df['vehicle_number'].unique())
                
                vehicle_comparison = []
                for vehicle in sorted(all_vehicles):
                    emp_qty = emp_df[emp_df['vehicle_number'] == vehicle]['weight_quintals'].astype(float).sum() if not emp_df.empty else 0
                    adm_qty = adm_df[adm_df['vehicle_number'] == vehicle]['quantity_quintals'].astype(float).sum() if not adm_df.empty else 0
                    vehicle_comparison.append({
                        'Vehicle': vehicle,
                        'Employee (Q)': round(emp_qty, 2),
                        'Admin (Q)': round(adm_qty, 2)
                    })
                
                if vehicle_comparison:
                    st.dataframe(pd.DataFrame(vehicle_comparison), use_container_width=True, hide_index=True)
            else:
                st.info("No data")
        
        # Backup Reminder
        st.markdown("---")
        st.info("💾 **Tip:** Download Excel backups regularly from each tab to keep your data safe!")
    
    # TAB 2: Admin Entry
    with tab2:
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
                # Use expander for mandi selection
                with st.expander("🏪 Select Mandi (click to expand)", expanded=False):
                    selected_mandis = st.multiselect("Choose one or more", mandi_options, key="admin_mandi", label_visibility="collapsed")
                selected_mandi = " + ".join(selected_mandis) if selected_mandis else None
                if selected_mandi:
                    st.success(f"📍 **{selected_mandi}**")
            else:
                selected_mandi = None
            
            vehicles_df = get_all_data(SHEET_VEHICLES)
            vehicles_df = vehicles_df[vehicles_df['is_active'].astype(str) == '1'] if not vehicles_df.empty else vehicles_df
            
            if not vehicles_df.empty:
                selected_vehicle = st.selectbox("Vehicle/Truck", [""] + vehicles_df['vehicle_number'].tolist(), key="admin_vehicle")
                selected_vehicle = selected_vehicle if selected_vehicle else None
            else:
                selected_vehicle = None
            
            ac_note = st.text_input("A/C Note Number", key="admin_ac_note")
            quantity = st.number_input("Quantity (Quintals)", min_value=0.0, value=0.0, step=0.1, key="admin_qty")
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
                        st.success("✅ Entry added!")
                        # Clear form fields by removing keys from session state
                        for key in ['admin_mandi', 'admin_vehicle', 'admin_ac_note', 'admin_qty', 'admin_remarks']:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.rerun()
        
        with list_col:
            st.markdown("##### 📋 Arrival Register")
            
            # Add filter options
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                admin_filter_start = st.date_input("From Date", value=date.today() - timedelta(days=7), key="admin_filter_start")
            with filter_col2:
                admin_filter_end = st.date_input("To Date", value=date.today(), key="admin_filter_end")
            
            admin_df = get_admin_arrivals(kms_year, start_date=admin_filter_start, end_date=admin_filter_end)
            
            if not admin_df.empty:
                # Edit mode check
                if 'edit_admin_id' not in st.session_state:
                    st.session_state['edit_admin_id'] = None
                
                # Get dropdown options for edit forms
                mandis_list = get_all_data(SHEET_MANDIS)['mandi_name'].tolist() if not get_all_data(SHEET_MANDIS).empty else []
                vehicles_list = get_all_data(SHEET_VEHICLES)
                vehicles_list = vehicles_list[vehicles_list['is_active'].astype(str) == '1']['vehicle_number'].tolist() if not vehicles_list.empty else []
                
                for date_val in admin_df['date'].unique():
                    day_data = admin_df[admin_df['date'] == date_val]
                    daily_total = day_data['quantity_quintals'].astype(float).sum()
                    
                    st.markdown(f"**📅 {date_val}** — Total: **{daily_total:.2f} Q**")
                    
                    for _, row in day_data.iterrows():
                        row_id = row['id']
                        
                        # Check if this row is being edited
                        if st.session_state['edit_admin_id'] == row_id:
                            # Edit form with dropdowns
                            with st.form(key=f"edit_admin_form_{row_id}"):
                                edit_cols = st.columns([2, 2, 1, 1])
                                
                                # Mandi dropdown
                                current_mandi = row['mandi_name']
                                mandi_idx = mandis_list.index(current_mandi) if current_mandi in mandis_list else 0
                                edit_mandi = edit_cols[0].selectbox("Mandi", mandis_list, index=mandi_idx, key=f"edit_mandi_{row_id}")
                                
                                # Vehicle dropdown
                                current_vehicle = row['vehicle_number']
                                vehicle_idx = vehicles_list.index(current_vehicle) if current_vehicle in vehicles_list else 0
                                edit_vehicle = edit_cols[1].selectbox("Vehicle", vehicles_list, index=vehicle_idx, key=f"edit_vehicle_{row_id}")
                                
                                edit_ac = edit_cols[2].text_input("A/C", value=row['ac_note'] or '', key=f"edit_ac_{row_id}")
                                edit_qty = edit_cols[3].number_input("Qty", value=float(row['quantity_quintals']), key=f"edit_qty_{row_id}")
                                
                                btn_cols = st.columns([1, 1, 2])
                                if btn_cols[0].form_submit_button("💾 Save"):
                                    update_row(SHEET_ADMIN_ARRIVALS, row_id, {
                                        "mandi_name": edit_mandi,
                                        "vehicle_number": edit_vehicle,
                                        "ac_note": edit_ac,
                                        "quantity_quintals": edit_qty
                                    })
                                    st.session_state['edit_admin_id'] = None
                                    st.rerun()
                                if btn_cols[1].form_submit_button("❌ Cancel"):
                                    st.session_state['edit_admin_id'] = None
                                    st.rerun()
                        else:
                            # Normal display
                            cols = st.columns([2, 2, 1, 1, 0.5, 0.5])
                            cols[0].write(f"🏪 {row['mandi_name']}")
                            cols[1].write(f"🚛 {row['vehicle_number']}")
                            cols[2].write(f"📝 {row['ac_note'] or '-'}")
                            cols[3].write(f"⚖️ {row['quantity_quintals']} Q")
                            
                            if cols[4].button("✏️", key=f"edit_{row_id}"):
                                st.session_state['edit_admin_id'] = row_id
                                st.rerun()
                            
                            if cols[5].button("🗑️", key=f"del_{row_id}"):
                                delete_row(SHEET_ADMIN_ARRIVALS, row_id)
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
    
    # TAB 3: Master Stock (Read-only, calculated from Admin Arrivals + Milling)
    with tab3:
        st.subheader("Master Stock Register (Read-Only)")
        st.caption("📊 Auto-calculated from Admin Arrivals and Milling entries")
        
        # Get admin arrivals
        admin_df = get_admin_arrivals(kms_year)
        milling_df = get_all_data(SHEET_MILLING)
        
        if not admin_df.empty:
            # Group admin arrivals by date
            admin_df['quantity_quintals'] = pd.to_numeric(admin_df['quantity_quintals'], errors='coerce')
            daily_received = admin_df.groupby('date')['quantity_quintals'].sum().reset_index()
            daily_received.columns = ['date', 'received']
            daily_received = daily_received.sort_values('date')
            
            # Get milling data by date
            milling_by_date = {}
            if not milling_df.empty:
                milling_kms = milling_df[milling_df['kms_year'] == kms_year]
                if not milling_kms.empty:
                    milling_kms['issued_quintals'] = pd.to_numeric(milling_kms['issued_quintals'], errors='coerce')
                    for _, row in milling_kms.groupby('date')['issued_quintals'].sum().reset_index().iterrows():
                        milling_by_date[row['date']] = row['issued_quintals']
            
            # Calculate master stock
            stock_data = []
            prog_received = 0
            prog_milling = 0
            prev_closing = 0
            
            for _, row in daily_received.iterrows():
                stock_date = row['date']
                received = row['received']
                prog_received += received
                issued = milling_by_date.get(stock_date, 0)
                prog_milling += issued
                
                opening = prev_closing
                total = opening + received
                closing = total - issued
                prev_closing = closing
                
                stock_data.append({
                    'Date': stock_date,
                    'O/B': round(opening, 2),
                    'Received': round(received, 2),
                    'Prog. Recv': round(prog_received, 2),
                    'Total': round(total, 2),
                    'Issue Mill': round(issued, 2),
                    'Prog. Mill': round(prog_milling, 2),
                    'C/B': round(closing, 2)
                })
            
            display_df = pd.DataFrame(stock_data)
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Received", f"{prog_received:.2f} Q")
            col2.metric("Total Issued", f"{prog_milling:.2f} Q")
            col3.metric("Current Stock", f"{prev_closing:.2f} Q")
            col4.metric("Prog. Received", f"{prog_received:.2f} Q")
            
            st.download_button(
                "📥 Download Stock Register",
                to_excel(display_df),
                f"master_stock_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No admin arrivals data. Add arrivals first.")
    
    # TAB 4: Milling (Issue for Milling entry)
    with tab4:
        st.subheader("Issue for Milling")
        
        entry_col, list_col = st.columns([1, 2])
        
        with entry_col:
            st.markdown("##### ➕ Add Milling Entry")
            
            mill_date = st.date_input("Date", value=date.today(), key="mill_date")
            mill_kms_year = get_kms_year_from_date(mill_date)
            st.caption(f"📅 KMS Year: **{mill_kms_year}**")
            
            mill_qty = st.number_input("Issued Quantity (Quintals)", min_value=0.0, value=0.0, step=0.1, key="mill_qty")
            mill_remarks = st.text_input("Remarks", key="mill_remarks")
            
            if st.button("💾 Add Milling Entry", type="primary", use_container_width=True):
                if mill_qty <= 0:
                    st.error("❌ Quantity must be greater than 0")
                else:
                    success = add_row(SHEET_MILLING, {
                        "id": get_next_id(SHEET_MILLING),
                        "date": str(mill_date),
                        "kms_year": mill_kms_year,
                        "issued_quintals": mill_qty,
                        "remarks": mill_remarks,
                        "entered_by": username,
                        "entry_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
                    if success:
                        st.success("✅ Milling entry added!")
                        st.rerun()
        
        with list_col:
            st.markdown("##### 📋 Milling Entries")
            
            milling_df = get_all_data(SHEET_MILLING)
            
            if not milling_df.empty:
                milling_kms = milling_df[milling_df['kms_year'] == kms_year].sort_values('date', ascending=False)
                
                if not milling_kms.empty:
                    for _, row in milling_kms.iterrows():
                        cols = st.columns([2, 2, 2, 1])
                        cols[0].write(f"📅 {row['date']}")
                        cols[1].write(f"⚖️ {row['issued_quintals']} Q")
                        cols[2].write(f"📝 {row.get('remarks', '') or '-'}")
                        
                        if cols[3].button("🗑️", key=f"del_mill_{row['id']}"):
                            delete_row(SHEET_MILLING, row['id'])
                            st.rerun()
                    
                    st.markdown("---")
                    total_milling = milling_kms['issued_quintals'].astype(float).sum()
                    st.markdown(f"### 📊 Total Issued: {total_milling:.2f} Q")
                else:
                    st.info("ℹ️ No milling entries for this KMS year.")
            else:
                st.info("ℹ️ No milling entries yet.")
    
    # TAB 5: Diesel
    with tab5:
        st.subheader("⛽ Diesel Entry")
        
        entry_col, summary_col = st.columns([1, 2])
        
        with entry_col:
            st.markdown("##### ➕ Add Diesel Entry")
            
            diesel_date = st.date_input("Date", value=date.today(), key="diesel_date")
            diesel_kms_year = get_kms_year_from_date(diesel_date)
            st.caption(f"📅 KMS Year: **{diesel_kms_year}**")
            
            vehicles_df = get_all_data(SHEET_VEHICLES)
            vehicles_df = vehicles_df[vehicles_df['is_active'].astype(str) == '1'] if not vehicles_df.empty else vehicles_df
            
            if not vehicles_df.empty:
                diesel_vehicle = st.selectbox("Vehicle", vehicles_df['vehicle_number'].tolist(), key="diesel_vehicle")
            else:
                diesel_vehicle = None
            
            diesel_liters = st.number_input("Liters", min_value=0.0, value=0.0, step=0.1, key="diesel_liters")
            diesel_amount = st.number_input("Amount (₹)", min_value=0.0, value=0.0, step=1.0, key="diesel_amount")
            diesel_pump = st.text_input("Pump/Station Name", key="diesel_pump")
            
            if st.button("💾 Add Diesel Entry", type="primary", use_container_width=True):
                if not diesel_vehicle:
                    st.error("❌ Please select a vehicle")
                elif diesel_liters <= 0:
                    st.error("❌ Liters must be greater than 0")
                elif diesel_amount <= 0:
                    st.error("❌ Amount must be greater than 0")
                else:
                    success = add_row(SHEET_DIESEL, {
                        "id": get_next_id(SHEET_DIESEL),
                        "date": str(diesel_date),
                        "kms_year": diesel_kms_year,
                        "vehicle_number": diesel_vehicle,
                        "liters": diesel_liters,
                        "amount": diesel_amount,
                        "pump_station": diesel_pump,
                        "entered_by": username,
                        "entry_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
                    if success:
                        st.success("✅ Diesel entry added!")
                        st.rerun()
        
        with summary_col:
            st.markdown("##### 📊 Diesel Summary")
            
            diesel_df = get_all_data(SHEET_DIESEL)
            
            if not diesel_df.empty:
                diesel_kms = diesel_df[diesel_df['kms_year'] == kms_year]
                
                if not diesel_kms.empty:
                    diesel_kms['liters'] = pd.to_numeric(diesel_kms['liters'], errors='coerce')
                    diesel_kms['amount'] = pd.to_numeric(diesel_kms['amount'], errors='coerce')
                    
                    # Overall totals
                    total_liters = diesel_kms['liters'].sum()
                    total_amount = diesel_kms['amount'].sum()
                    
                    col1, col2 = st.columns(2)
                    col1.metric("Total Liters", f"{total_liters:,.2f} L")
                    col2.metric("Total Amount", f"₹{total_amount:,.2f}")
                    
                    st.markdown("---")
                    
                    # By Vehicle
                    st.markdown("##### By Vehicle")
                    by_vehicle = diesel_kms.groupby('vehicle_number').agg({
                        'liters': 'sum',
                        'amount': 'sum'
                    }).sort_values('amount', ascending=False)
                    by_vehicle.columns = ['Liters', 'Amount (₹)']
                    st.dataframe(by_vehicle, use_container_width=True)
                    
                    # By Month
                    st.markdown("##### By Month")
                    diesel_kms['month'] = diesel_kms['date'].str[:7]
                    by_month = diesel_kms.groupby('month').agg({
                        'liters': 'sum',
                        'amount': 'sum'
                    }).sort_index(ascending=False)
                    by_month.columns = ['Liters', 'Amount (₹)']
                    st.dataframe(by_month, use_container_width=True)
                    
                    st.markdown("---")
                    
                    # Recent entries with edit/delete
                    st.markdown("##### Recent Entries")
                    for _, row in diesel_kms.sort_values('date', ascending=False).head(20).iterrows():
                        cols = st.columns([1.5, 2, 1, 1.5, 2, 0.5, 0.5])
                        cols[0].write(f"📅 {row['date']}")
                        cols[1].write(f"🚛 {row['vehicle_number']}")
                        cols[2].write(f"⛽ {row['liters']} L")
                        cols[3].write(f"₹{row['amount']}")
                        cols[4].write(f"🏪 {row['pump_station'] or '-'}")
                        
                        if cols[5].button("✏️", key=f"edit_diesel_{row['id']}"):
                            st.session_state['edit_diesel_id'] = row['id']
                            st.rerun()
                        
                        if cols[6].button("🗑️", key=f"del_diesel_{row['id']}"):
                            delete_row(SHEET_DIESEL, row['id'])
                            st.rerun()
                    
                    # Download
                    st.download_button(
                        "📥 Download Diesel Data",
                        to_excel(diesel_kms[['date', 'vehicle_number', 'liters', 'amount', 'pump_station']]),
                        f"diesel_{kms_year}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.info("ℹ️ No diesel entries for this KMS year.")
            else:
                st.info("ℹ️ No diesel entries yet.")
    
    # TAB 6: Employee Data
    with tab6:
        st.subheader("Employee Entries")
        
        # Add search/filter
        col1, col2, col3 = st.columns(3)
        with col1:
            search_vehicle = st.text_input("🔍 Search Vehicle", key="search_emp_vehicle")
        with col2:
            search_mandi = st.text_input("🔍 Search Mandi", key="search_emp_mandi")
        with col3:
            filter_date = st.date_input("📅 Filter Date", value=None, key="filter_emp_date")
        
        emp_entries = get_employee_arrivals(kms_year)
        
        if not emp_entries.empty:
            # Apply filters
            filtered_emp = emp_entries.copy()
            if search_vehicle:
                filtered_emp = filtered_emp[filtered_emp['vehicle_number'].str.contains(search_vehicle.upper(), na=False)]
            if search_mandi:
                filtered_emp = filtered_emp[filtered_emp['mandi_name'].str.contains(search_mandi.upper(), na=False)]
            if filter_date:
                filtered_emp = filtered_emp[filtered_emp['date'] == str(filter_date)]
            
            st.dataframe(filtered_emp[['date', 'mandi_name', 'vehicle_number', 'bags',
                                      'weight_quintals', 'godown', 'difference', 'entered_by']],
                        use_container_width=True, hide_index=True)
            
            st.caption(f"Showing {len(filtered_emp)} of {len(emp_entries)} entries")
        else:
            st.info("ℹ️ No employee entries.")
    
    # TAB 7: Vehicles
    with tab7:
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
    
    # TAB 8: Settings
    with tab8:
        settings_tab1, settings_tab2, settings_tab3 = st.tabs(["🏪 Mandis", "🏭 Godowns", "👥 Users"])
        
        with settings_tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### ➕ Add Mandi")
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
                
                st.markdown("---")
                st.markdown("##### 🗑️ Delete Mandi")
                mandis_df = get_all_data(SHEET_MANDIS)
                if not mandis_df.empty:
                    del_mandi = st.selectbox("Select Mandi to Delete", mandis_df['mandi_name'].tolist(), key="del_mandi")
                    if st.button("🗑️ Delete Mandi", type="secondary"):
                        mandi_id = mandis_df[mandis_df['mandi_name'] == del_mandi]['id'].values[0]
                        delete_row(SHEET_MANDIS, mandi_id)
                        st.success("✅ Deleted!")
                        st.rerun()
            
            with col2:
                st.markdown("##### 📋 Current Mandis")
                if not mandis_df.empty:
                    st.dataframe(mandis_df[['mandi_name']], use_container_width=True, hide_index=True)
        
        with settings_tab2:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### ➕ Add Godown")
                new_godown = st.text_input("New Godown Name")
                if st.button("➕ Add Godown", type="primary"):
                    if new_godown:
                        add_row(SHEET_GODOWNS, {
                            "id": get_next_id(SHEET_GODOWNS),
                            "godown_name": new_godown.strip()
                        })
                        st.success("✅ Added!")
                        st.rerun()
                
                st.markdown("---")
                st.markdown("##### 🗑️ Delete Godown")
                godowns_df = get_all_data(SHEET_GODOWNS)
                if not godowns_df.empty:
                    del_godown = st.selectbox("Select Godown to Delete", godowns_df['godown_name'].tolist(), key="del_godown")
                    if st.button("🗑️ Delete Godown", type="secondary"):
                        godown_id = godowns_df[godowns_df['godown_name'] == del_godown]['id'].values[0]
                        delete_row(SHEET_GODOWNS, godown_id)
                        st.success("✅ Deleted!")
                        st.rerun()
            
            with col2:
                st.markdown("##### 📋 Current Godowns")
                if not godowns_df.empty:
                    st.dataframe(godowns_df[['godown_name']], use_container_width=True, hide_index=True)
        
        with settings_tab3:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### ➕ Add New User")
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
                
                st.markdown("---")
                st.markdown("##### 🗑️ Delete User")
                users_df = get_all_data(SHEET_USERS)
                if not users_df.empty:
                    del_users = users_df[users_df['username'] != username]['username'].tolist()
                    if del_users:
                        del_user = st.selectbox("Select User to Delete", del_users, key="del_user")
                        if st.button("🗑️ Delete User", type="secondary"):
                            user_id = users_df[users_df['username'] == del_user]['id'].values[0]
                            delete_row(SHEET_USERS, user_id)
                            st.success("✅ Deleted!")
                            st.rerun()
                    else:
                        st.info("No other users to delete")
                
                st.markdown("---")
                st.markdown("##### 🔑 Reset Password")
                if not users_df.empty:
                    reset_user = st.selectbox("Select User", users_df['username'].tolist(), key="reset_user")
                    new_pass = st.text_input("New Password", type="password", key="new_pass")
                    if st.button("🔑 Reset Password"):
                        if new_pass:
                            user_id = users_df[users_df['username'] == reset_user]['id'].values[0]
                            update_row(SHEET_USERS, user_id, {"password_hash": hash_password(new_pass)})
                            st.success("✅ Password reset!")
                        else:
                            st.error("Enter new password")
            
            with col2:
                st.markdown("##### 📋 Current Users")
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
