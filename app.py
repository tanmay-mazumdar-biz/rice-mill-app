"""
Rice Mill Procurement System v2.0
A comprehensive Streamlit application with Employee/Admin roles,
dual data tracking, and advanced reporting features.
"""

import streamlit as st
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime, date, timedelta
from io import BytesIO

# Page configuration
st.set_page_config(
    page_title="Rice Mill Procurement System",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
DB_PATH = "rice_mill_v2.db"
WEIGHT_PER_BAG = 0.51  # Quintals per bag (51 kg)
DIFFERENCE_THRESHOLD = 2.0  # Quintals - flag if difference exceeds this

# ============== DATABASE FUNCTIONS ==============

def get_connection():
    """Create database connection"""
    return sqlite3.connect(DB_PATH)

def init_database():
    """Initialize SQLite database with all required tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin', 'employee')),
            full_name TEXT,
            phone TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Settings_Mandi table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Settings_Mandi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandi_name TEXT UNIQUE NOT NULL,
            distance_km REAL DEFAULT 0
        )
    """)
    
    # Settings_Godown table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Settings_Godown (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            godown_name TEXT UNIQUE NOT NULL
        )
    """)
    
    # Vehicle_Registry table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Vehicle_Registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_number TEXT UNIQUE NOT NULL,
            owner_name TEXT,
            puc_expiry_date DATE,
            permit_number TEXT,
            is_active INTEGER DEFAULT 1
        )
    """)
    
    # Employee_Arrivals table (Field data)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Employee_Arrivals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            kms_year TEXT NOT NULL,
            mandi_name TEXT NOT NULL,
            vehicle_number TEXT NOT NULL,
            bags INTEGER NOT NULL,
            weight_quintals REAL NOT NULL,
            godown TEXT,
            expected_weight REAL,
            difference REAL,
            entered_by TEXT NOT NULL,
            entry_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            remarks TEXT
        )
    """)
    
    # Admin_Arrivals table (Official register - Book 1)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Admin_Arrivals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            kms_year TEXT NOT NULL,
            mandi_name TEXT NOT NULL,
            vehicle_number TEXT NOT NULL,
            ac_note TEXT,
            quantity_quintals REAL NOT NULL,
            entered_by TEXT NOT NULL,
            entry_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            remarks TEXT
        )
    """)
    
    # Master_Stock table (Book 2 - Daily summary)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Master_Stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            kms_year TEXT NOT NULL,
            opening_balance REAL DEFAULT 0,
            received_today REAL DEFAULT 0,
            prog_received REAL DEFAULT 0,
            total REAL DEFAULT 0,
            issued_milling REAL DEFAULT 0,
            prog_milling REAL DEFAULT 0,
            closing_balance REAL DEFAULT 0,
            remarks TEXT,
            UNIQUE(date, kms_year)
        )
    """)
    
    # Create default admin user if not exists
    cursor.execute("SELECT COUNT(*) FROM Users WHERE role = 'admin'")
    if cursor.fetchone()[0] == 0:
        admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
        cursor.execute("""
            INSERT INTO Users (username, password_hash, role, full_name)
            VALUES (?, ?, 'admin', 'Administrator')
        """, ("admin", admin_hash))
    
    # Create default employee accounts if not exists
    cursor.execute("SELECT COUNT(*) FROM Users WHERE role = 'employee'")
    if cursor.fetchone()[0] == 0:
        emp_hash = hashlib.sha256("emp123".encode()).hexdigest()
        cursor.execute("""
            INSERT INTO Users (username, password_hash, role, full_name)
            VALUES (?, ?, 'employee', 'Employee 1')
        """, ("emp1", emp_hash))
        cursor.execute("""
            INSERT INTO Users (username, password_hash, role, full_name)
            VALUES (?, ?, 'employee', 'Employee 2')
        """, ("emp2", emp_hash))
    
    # Create default godowns if not exists
    cursor.execute("SELECT COUNT(*) FROM Settings_Godown")
    if cursor.fetchone()[0] == 0:
        for godown in ['Hoper', 'G-3', 'S-2']:
            cursor.execute("INSERT OR IGNORE INTO Settings_Godown (godown_name) VALUES (?)", (godown,))
    
    # Create default mandis if not exists
    cursor.execute("SELECT COUNT(*) FROM Settings_Mandi")
    if cursor.fetchone()[0] == 0:
        mandis = ['BHEJENGIWADA', 'CHALANGUDA', 'GUMKA', 'KALIMELA', 'M.V-11', 'M.V-26',
                  'MARIWADA', 'MARKAPALLY', 'MATAPAKA', 'PUSUGUDA', 'UDDUPA']
        for mandi in mandis:
            cursor.execute("INSERT OR IGNORE INTO Settings_Mandi (mandi_name, distance_km) VALUES (?, 0)", (mandi,))
    
    # Create default vehicles if not exists
    cursor.execute("SELECT COUNT(*) FROM Vehicle_Registry")
    if cursor.fetchone()[0] == 0:
        vehicles = ['AP31TU1719', 'CG08Z6713', 'CG17KL6229', 'OD30A9549', 'OD30B3879',
                    'OD30B5356', 'OD30H0487', 'OR10C5722', 'OR301611']
        for vehicle in vehicles:
            cursor.execute("INSERT OR IGNORE INTO Vehicle_Registry (vehicle_number) VALUES (?)", (vehicle,))
    
    conn.commit()
    conn.close()

def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_login(username, password):
    """Verify user credentials"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, username, role, full_name FROM Users 
        WHERE username = ? AND password_hash = ? AND is_active = 1
    """, (username, hash_password(password)))
    user = cursor.fetchone()
    conn.close()
    return user

def get_users():
    """Fetch all users"""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT id, username, role, full_name, phone, is_active, created_at 
        FROM Users ORDER BY role, username
    """, conn)
    conn.close()
    return df

def get_mandis():
    """Fetch all mandis"""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Settings_Mandi ORDER BY mandi_name", conn)
    conn.close()
    return df

def get_godowns():
    """Fetch all godowns"""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Settings_Godown ORDER BY godown_name", conn)
    conn.close()
    return df

def get_vehicles(active_only=True):
    """Fetch vehicles"""
    conn = get_connection()
    query = "SELECT * FROM Vehicle_Registry"
    if active_only:
        query += " WHERE is_active = 1"
    query += " ORDER BY vehicle_number"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if not df.empty and 'puc_expiry_date' in df.columns:
        df['puc_expiry_date'] = pd.to_datetime(df['puc_expiry_date'], errors='coerce').dt.date
    return df

def get_employee_arrivals(kms_year, user=None, start_date=None, end_date=None):
    """Fetch employee arrivals with optional filters"""
    conn = get_connection()
    query = "SELECT * FROM Employee_Arrivals WHERE kms_year = ?"
    params = [kms_year]
    
    if user:
        query += " AND entered_by = ?"
        params.append(user)
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date DESC, id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def get_admin_arrivals(kms_year, start_date=None, end_date=None):
    """Fetch admin arrivals with optional filters"""
    conn = get_connection()
    query = "SELECT * FROM Admin_Arrivals WHERE kms_year = ?"
    params = [kms_year]
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date DESC, id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def get_master_stock(kms_year):
    """Fetch master stock for KMS year"""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT * FROM Master_Stock WHERE kms_year = ? ORDER BY date ASC
    """, conn, params=(kms_year,))
    conn.close()
    return df

def update_master_stock(kms_year):
    """Recalculate entire master stock from admin arrivals"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get all admin arrivals grouped by date
        cursor.execute("""
            SELECT date, SUM(quantity_quintals) as received
            FROM Admin_Arrivals 
            WHERE kms_year = ?
            GROUP BY date
            ORDER BY date ASC
        """, (kms_year,))
        daily_arrivals = cursor.fetchall()
        
        # Get existing issued_milling values to preserve them
        cursor.execute("""
            SELECT date, issued_milling, remarks FROM Master_Stock WHERE kms_year = ?
        """, (kms_year,))
        existing_data = {row[0]: {'issued': row[1], 'remarks': row[2]} for row in cursor.fetchall()}
        
        # Clear and recalculate
        prog_received = 0
        prog_milling = 0
        prev_closing = 0
        
        for arrival_date, received in daily_arrivals:
            prog_received += received
            issued = existing_data.get(arrival_date, {}).get('issued', 0) or 0
            prog_milling += issued
            remarks = existing_data.get(arrival_date, {}).get('remarks', '')
            
            opening = prev_closing
            total = opening + received
            closing = total - issued
            prev_closing = closing
            
            cursor.execute("""
                INSERT INTO Master_Stock (date, kms_year, opening_balance, received_today, 
                    prog_received, total, issued_milling, prog_milling, closing_balance, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, kms_year) DO UPDATE SET
                    opening_balance = excluded.opening_balance,
                    received_today = excluded.received_today,
                    prog_received = excluded.prog_received,
                    total = excluded.total,
                    prog_milling = excluded.prog_milling,
                    closing_balance = excluded.closing_balance
            """, (arrival_date, kms_year, opening, received, prog_received, total, 
                  issued, prog_milling, closing, remarks))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Error updating master stock: {e}")
        return False
    finally:
        conn.close()

def check_duplicate_entry(table, date_val, vehicle, mandi, kms_year, exclude_id=None):
    """Check for duplicate entries"""
    conn = get_connection()
    cursor = conn.cursor()
    query = f"""
        SELECT COUNT(*) FROM {table} 
        WHERE date = ? AND vehicle_number = ? AND mandi_name = ? AND kms_year = ?
    """
    params = [date_val, vehicle, mandi, kms_year]
    
    if exclude_id:
        query += " AND id != ?"
        params.append(exclude_id)
    
    cursor.execute(query, params)
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def to_excel(df):
    """Convert dataframe to Excel bytes"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
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
                <p>Default Admin Login: admin / admin123</p>
            </div>
        """, unsafe_allow_html=True)

def show_sidebar():
    """Display sidebar with user info and filters"""
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state['full_name']}")
        st.markdown(f"**Role:** {'🔑 Admin' if st.session_state['role'] == 'admin' else '👷 Employee'}")
        
        st.markdown("---")
        
        # KMS Year Selection
        st.subheader("📅 KMS Year")
        current_year = datetime.now().year
        year_options = [f"{y}-{str(y+1)[-2:]}" for y in range(current_year-2, current_year+3)]
        kms_year = st.selectbox("Select Year", year_options, index=2, label_visibility="collapsed")
        st.session_state['kms_year'] = kms_year
        
        st.markdown("---")
        
        # Logout button
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
            
            # Check if date is too old
            days_old = (date.today() - entry_date).days
            if days_old > 7:
                st.warning(f"⚠️ Entry is {days_old} days old. Please verify.")
            
            mandis_df = get_mandis()
            if not mandis_df.empty:
                mandi_options = mandis_df['mandi_name'].tolist()
                selected_mandis = st.multiselect("Mandi (select one or more)", mandi_options)
                selected_mandi = " + ".join(selected_mandis) if selected_mandis else None
                if selected_mandi:
                    st.info(f"📍 Selected: **{selected_mandi}**")
            else:
                st.warning("⚠️ No mandis configured. Contact admin.")
                selected_mandi = None
            
            bags = st.number_input("Number of Bags", min_value=0, value=0, step=1)
        
        with col2:
            vehicles_df = get_vehicles()
            if not vehicles_df.empty:
                vehicle_options = vehicles_df['vehicle_number'].tolist()
                selected_vehicle = st.selectbox("Vehicle Number", vehicle_options)
                
                # Check PUC expiry
                vehicle_info = vehicles_df[vehicles_df['vehicle_number'] == selected_vehicle].iloc[0]
                if pd.notna(vehicle_info['puc_expiry_date']):
                    if vehicle_info['puc_expiry_date'] < date.today():
                        st.error(f"🚨 PUC EXPIRED on {vehicle_info['puc_expiry_date']}")
                    elif vehicle_info['puc_expiry_date'] < date.today() + timedelta(days=30):
                        st.warning(f"⚠️ PUC expiring on {vehicle_info['puc_expiry_date']}")
            else:
                st.warning("⚠️ No vehicles registered. Contact admin.")
                selected_vehicle = None
            
            godowns_df = get_godowns()
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
                st.warning(f"⚠️ Difference exceeds threshold of ±{DIFFERENCE_THRESHOLD} Q")
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
                # Check for duplicate
                if check_duplicate_entry("Employee_Arrivals", entry_date, selected_vehicle, 
                                        selected_mandi, kms_year):
                    st.warning("⚠️ Similar entry exists for this date/vehicle/mandi. Submit anyway?")
                    if st.button("Yes, Submit", key="confirm_dup"):
                        pass  # Continue below
                    else:
                        st.stop()
                
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO Employee_Arrivals 
                        (date, kms_year, mandi_name, vehicle_number, bags, weight_quintals,
                         godown, expected_weight, difference, entered_by, remarks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (entry_date, kms_year, selected_mandi, selected_vehicle, bags,
                          weight, selected_godown, expected_weight, difference, username, remarks))
                    conn.commit()
                    st.success("✅ Entry saved successfully!")
                    st.balloons()
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ Error: {e}")
                finally:
                    conn.close()
    
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
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Entries", len(entries_df))
            col2.metric("Total Bags", f"{entries_df['bags'].sum():,.0f}")
            col3.metric("Total Weight", f"{entries_df['weight_quintals'].sum():,.2f} Q")
            col4.metric("Avg Difference", f"{entries_df['difference'].mean():+.2f} Q")
            
            st.markdown("---")
            
            # Display entries
            for idx, row in entries_df.iterrows():
                with st.container():
                    cols = st.columns([2, 2, 2, 1, 1, 1, 1])
                    cols[0].write(f"**📅 {row['date']}**")
                    cols[1].write(f"🏪 {row['mandi_name']}")
                    cols[2].write(f"🚛 {row['vehicle_number']}")
                    cols[3].write(f"📦 {row['bags']}")
                    cols[4].write(f"⚖️ {row['weight_quintals']} Q")
                    cols[5].write(f"🏭 {row['godown'] or '-'}")
                    
                    diff = row['difference']
                    if abs(diff) > DIFFERENCE_THRESHOLD:
                        cols[6].write(f"⚠️ {diff:+.2f}")
                    else:
                        cols[6].write(f"✅ {diff:+.2f}")
                    
                    st.divider()
            
            # Download button
            export_df = entries_df[['date', 'mandi_name', 'vehicle_number', 'bags', 
                                    'weight_quintals', 'godown', 'expected_weight', 
                                    'difference', 'remarks']].copy()
            export_df.columns = ['Date', 'Mandi', 'Vehicle', 'Bags', 'Weight (Q)', 
                                'Godown', 'Expected (Q)', 'Difference', 'Remarks']
            
            st.download_button(
                "📥 Download My Entries (Excel)",
                to_excel(export_df),
                f"my_entries_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No entries found for the selected period.")
    
    # TAB 3: My Summary
    with tab3:
        st.subheader("My Performance Summary")
        
        all_entries = get_employee_arrivals(kms_year, username)
        
        if not all_entries.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### 📊 By Mandi")
                mandi_summary = all_entries.groupby('mandi_name').agg({
                    'bags': 'sum',
                    'weight_quintals': 'sum',
                    'id': 'count'
                }).rename(columns={'id': 'trips'}).sort_values('weight_quintals', ascending=False)
                st.dataframe(mandi_summary, use_container_width=True)
            
            with col2:
                st.markdown("##### 🚛 By Vehicle")
                vehicle_summary = all_entries.groupby('vehicle_number').agg({
                    'bags': 'sum',
                    'weight_quintals': 'sum',
                    'id': 'count'
                }).rename(columns={'id': 'trips'}).sort_values('weight_quintals', ascending=False)
                st.dataframe(vehicle_summary, use_container_width=True)
            
            # Difference analysis
            st.markdown("---")
            st.markdown("##### ⚖️ Weight Difference Analysis")
            
            high_diff = all_entries[abs(all_entries['difference']) > DIFFERENCE_THRESHOLD]
            if not high_diff.empty:
                st.warning(f"⚠️ {len(high_diff)} entries with difference > ±{DIFFERENCE_THRESHOLD} Q")
                st.dataframe(high_diff[['date', 'mandi_name', 'vehicle_number', 'bags', 
                                        'weight_quintals', 'expected_weight', 'difference']], 
                            use_container_width=True)
            else:
                st.success("✅ All entries within acceptable difference range")
        else:
            st.info("ℹ️ No entries found for this KMS year.")

def show_admin_dashboard():
    """Display admin dashboard"""
    st.title("🌾 Rice Mill - Admin Portal")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📝 Admin Entry", 
        "📊 Master Stock", 
        "🔄 Comparison",
        "👁️ Employee Data",
        "🚛 Vehicles",
        "⚙️ Settings"
    ])
    
    kms_year = st.session_state['kms_year']
    username = st.session_state['username']
    
    # TAB 1: Admin Entry (Book 1)
    with tab1:
        st.subheader("Official Arrival Register")
        
        entry_col, list_col = st.columns([1, 2])
        
        with entry_col:
            st.markdown("##### ➕ Add Entry")
            
            entry_date = st.date_input("Date", value=date.today(), key="admin_date")
            
            mandis_df = get_mandis()
            if not mandis_df.empty:
                mandi_options = mandis_df['mandi_name'].tolist()
                selected_mandis = st.multiselect("Mandi (select one or more)", mandi_options, key="admin_mandi")
                selected_mandi = " + ".join(selected_mandis) if selected_mandis else None
                if selected_mandi:
                    st.info(f"📍 Selected: **{selected_mandi}**")
            else:
                selected_mandi = None
            
            vehicles_df = get_vehicles()
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
                    conn = get_connection()
                    cursor = conn.cursor()
                    try:
                        cursor.execute("""
                            INSERT INTO Admin_Arrivals 
                            (date, kms_year, mandi_name, vehicle_number, ac_note, 
                             quantity_quintals, entered_by, remarks)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (entry_date, kms_year, selected_mandi, selected_vehicle,
                              ac_note, quantity, username, remarks))
                        conn.commit()
                        
                        # Update master stock
                        update_master_stock(kms_year)
                        
                        st.success("✅ Entry added!")
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error: {e}")
                    finally:
                        conn.close()
        
        with list_col:
            st.markdown("##### 📋 Arrival Register")
            
            admin_df = get_admin_arrivals(kms_year)
            
            if not admin_df.empty:
                # Group by date with daily totals
                for date_val in admin_df['date'].unique():
                    day_data = admin_df[admin_df['date'] == date_val]
                    daily_total = day_data['quantity_quintals'].sum()
                    
                    st.markdown(f"**📅 {date_val}** — Daily Total: **{daily_total:.2f} Q**")
                    
                    for _, row in day_data.iterrows():
                        cols = st.columns([2, 2, 1, 1, 1])
                        cols[0].write(f"🏪 {row['mandi_name']}")
                        cols[1].write(f"🚛 {row['vehicle_number']}")
                        cols[2].write(f"📝 {row['ac_note'] or '-'}")
                        cols[3].write(f"⚖️ {row['quantity_quintals']} Q")
                        
                        if cols[4].button("🗑️", key=f"del_admin_{row['id']}", help="Delete"):
                            conn = get_connection()
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM Admin_Arrivals WHERE id = ?", (row['id'],))
                            conn.commit()
                            conn.close()
                            update_master_stock(kms_year)
                            st.rerun()
                    
                    st.divider()
                
                # Running total
                total_qty = admin_df['quantity_quintals'].sum()
                st.markdown(f"### 📊 Total: {total_qty:.2f} Quintals")
                
                # Export
                export_df = admin_df[['date', 'mandi_name', 'vehicle_number', 'ac_note', 
                                      'quantity_quintals', 'remarks']].copy()
                export_df.columns = ['Date', 'Mandi', 'Vehicle', 'A/C Note', 'Quantity (Q)', 'Remarks']
                
                st.download_button(
                    "📥 Download Register (Excel)",
                    to_excel(export_df),
                    f"admin_register_{kms_year}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("ℹ️ No entries yet for this KMS year.")
    
    # TAB 2: Master Stock (Book 2)
    with tab2:
        st.subheader("Master Stock Register")
        
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔄 Recalculate", use_container_width=True):
                update_master_stock(kms_year)
                st.success("✅ Stock recalculated!")
                st.rerun()
        
        stock_df = get_master_stock(kms_year)
        
        if not stock_df.empty:
            # Display as editable table
            display_df = stock_df[['date', 'opening_balance', 'received_today', 'prog_received',
                                   'total', 'issued_milling', 'prog_milling', 'closing_balance']].copy()
            display_df.columns = ['Date', 'O/B', 'Received', 'Prog. Received', 'Total', 
                                 'Issued Milling', 'Prog. Milling', 'C/B']
            
            edited_df = st.data_editor(
                display_df,
                use_container_width=True,
                hide_index=True,
                disabled=['Date', 'O/B', 'Received', 'Prog. Received', 'Total', 'Prog. Milling', 'C/B'],
                column_config={
                    "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                    "O/B": st.column_config.NumberColumn("O/B", format="%.2f"),
                    "Received": st.column_config.NumberColumn("Received", format="%.2f"),
                    "Prog. Received": st.column_config.NumberColumn("Prog. Recv", format="%.2f"),
                    "Total": st.column_config.NumberColumn("Total", format="%.2f"),
                    "Issued Milling": st.column_config.NumberColumn("Issue Mill", format="%.2f"),
                    "Prog. Milling": st.column_config.NumberColumn("Prog. Mill", format="%.2f"),
                    "C/B": st.column_config.NumberColumn("C/B", format="%.2f"),
                }
            )
            
            if st.button("💾 Save Milling Changes", type="primary"):
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    for idx, row in edited_df.iterrows():
                        stock_date = stock_df.iloc[idx]['date']
                        issued = row['Issued Milling']
                        cursor.execute("""
                            UPDATE Master_Stock SET issued_milling = ?
                            WHERE date = ? AND kms_year = ?
                        """, (issued, stock_date, kms_year))
                    conn.commit()
                    update_master_stock(kms_year)
                    st.success("✅ Saved!")
                    st.rerun()
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ Error: {e}")
                finally:
                    conn.close()
            
            # Summary
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Received", f"{display_df['Received'].sum():.2f} Q")
            col2.metric("Total Issued", f"{display_df['Issued Milling'].sum():.2f} Q")
            col3.metric("Current Stock", f"{display_df['C/B'].iloc[-1]:.2f} Q")
            col4.metric("Prog. Received", f"{display_df['Prog. Received'].iloc[-1]:.2f} Q")
            
            # Export
            st.download_button(
                "📥 Download Stock Register (Excel)",
                to_excel(display_df),
                f"master_stock_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No stock data. Add arrivals in the Admin Entry tab.")
    
    # TAB 3: Comparison
    with tab3:
        st.subheader("Employee vs Admin Data Comparison")
        
        col1, col2 = st.columns(2)
        with col1:
            comp_start = st.date_input("From", value=date.today() - timedelta(days=30), key="comp_start")
        with col2:
            comp_end = st.date_input("To", value=date.today(), key="comp_end")
        
        emp_df = get_employee_arrivals(kms_year, start_date=comp_start, end_date=comp_end)
        adm_df = get_admin_arrivals(kms_year, start_date=comp_start, end_date=comp_end)
        
        if not emp_df.empty or not adm_df.empty:
            # Daily comparison
            emp_daily = emp_df.groupby('date')['weight_quintals'].sum() if not emp_df.empty else pd.Series()
            adm_daily = adm_df.groupby('date')['quantity_quintals'].sum() if not adm_df.empty else pd.Series()
            
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
                    'Date': d,
                    'Employee (Q)': emp_val,
                    'Admin (Q)': adm_val,
                    'Difference': diff,
                    'Status': status
                })
            
            comp_df = pd.DataFrame(comparison_data)
            
            # Summary
            col1, col2, col3 = st.columns(3)
            col1.metric("Employee Total", f"{emp_daily.sum():.2f} Q")
            col2.metric("Admin Total", f"{adm_daily.sum():.2f} Q")
            col3.metric("Difference", f"{emp_daily.sum() - adm_daily.sum():+.2f} Q")
            
            st.markdown("---")
            
            # Display comparison table
            st.dataframe(
                comp_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                    "Employee (Q)": st.column_config.NumberColumn("Employee (Q)", format="%.2f"),
                    "Admin (Q)": st.column_config.NumberColumn("Admin (Q)", format="%.2f"),
                    "Difference": st.column_config.NumberColumn("Difference", format="%+.2f"),
                }
            )
            
            # Export
            st.download_button(
                "📥 Download Comparison (Excel)",
                to_excel(comp_df),
                f"comparison_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No data available for comparison.")
    
    # TAB 4: Employee Data (Read-only view)
    with tab4:
        st.subheader("Employee Entries (Reference)")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            emp_start = st.date_input("From", value=date.today() - timedelta(days=7), key="emp_view_start")
        with col2:
            emp_end = st.date_input("To", value=date.today(), key="emp_view_end")
        with col3:
            users_df = get_users()
            emp_users = users_df[users_df['role'] == 'employee']['username'].tolist()
            emp_users.insert(0, "All Employees")
            selected_emp = st.selectbox("Employee", emp_users)
        
        user_filter = None if selected_emp == "All Employees" else selected_emp
        emp_entries = get_employee_arrivals(kms_year, user_filter, emp_start, emp_end)
        
        if not emp_entries.empty:
            # Summary
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Entries", len(emp_entries))
            col2.metric("Total Bags", f"{emp_entries['bags'].sum():,.0f}")
            col3.metric("Total Weight", f"{emp_entries['weight_quintals'].sum():,.2f} Q")
            col4.metric("Avg Diff", f"{emp_entries['difference'].mean():+.2f} Q")
            
            st.markdown("---")
            
            # Display
            display_emp = emp_entries[['date', 'mandi_name', 'vehicle_number', 'bags', 
                                       'weight_quintals', 'godown', 'difference', 'entered_by']].copy()
            display_emp.columns = ['Date', 'Mandi', 'Vehicle', 'Bags', 'Weight (Q)', 
                                   'Godown', 'Diff', 'Entered By']
            
            st.dataframe(display_emp, use_container_width=True, hide_index=True)
            
            # Export
            st.download_button(
                "📥 Download Employee Data (Excel)",
                to_excel(display_emp),
                f"employee_data_{kms_year}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No employee entries for selected filters.")
    
    # TAB 5: Vehicles
    with tab5:
        st.subheader("Vehicle Registry")
        
        vcol1, vcol2 = st.columns([1, 2])
        
        with vcol1:
            st.markdown("##### ➕ Add Vehicle")
            
            new_vehicle = st.text_input("Vehicle Number")
            new_owner = st.text_input("Owner Name")
            new_puc = st.date_input("PUC Expiry Date", key="new_puc")
            new_permit = st.text_input("Permit Number")
            
            if st.button("➕ Add Vehicle", type="primary", use_container_width=True):
                if not new_vehicle:
                    st.error("❌ Vehicle number required")
                else:
                    conn = get_connection()
                    cursor = conn.cursor()
                    try:
                        cursor.execute("""
                            INSERT INTO Vehicle_Registry (vehicle_number, owner_name, puc_expiry_date, permit_number)
                            VALUES (?, ?, ?, ?)
                        """, (new_vehicle.strip().upper(), new_owner, new_puc, new_permit))
                        conn.commit()
                        st.success("✅ Vehicle added!")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("❌ Vehicle already exists")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error: {e}")
                    finally:
                        conn.close()
        
        with vcol2:
            st.markdown("##### 📋 Registered Vehicles")
            
            vehicles_df = get_vehicles(active_only=False)
            
            if not vehicles_df.empty:
                # Check for expiring PUCs
                today = date.today()
                for _, v in vehicles_df.iterrows():
                    if pd.notna(v['puc_expiry_date']):
                        if v['puc_expiry_date'] < today:
                            st.error(f"🚨 {v['vehicle_number']} - PUC EXPIRED ({v['puc_expiry_date']})")
                        elif v['puc_expiry_date'] < today + timedelta(days=30):
                            st.warning(f"⚠️ {v['vehicle_number']} - PUC expiring ({v['puc_expiry_date']})")
                
                display_vehicles = vehicles_df[['vehicle_number', 'owner_name', 'puc_expiry_date', 'permit_number']].copy()
                
                st.dataframe(display_vehicles, use_container_width=True, hide_index=True)
                
                # Delete vehicle
                st.markdown("---")
                del_vehicle = st.selectbox("Select vehicle to delete", vehicles_df['vehicle_number'].tolist())
                if st.button("🗑️ Delete Vehicle", type="secondary"):
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM Vehicle_Registry WHERE vehicle_number = ?", (del_vehicle,))
                    conn.commit()
                    conn.close()
                    st.success("✅ Deleted!")
                    st.rerun()
            else:
                st.info("ℹ️ No vehicles registered.")
    
    # TAB 6: Settings
    with tab6:
        settings_tab1, settings_tab2, settings_tab3 = st.tabs(["🏪 Mandis", "🏭 Godowns", "👥 Users"])
        
        # Mandis
        with settings_tab1:
            st.markdown("##### Mandi Management")
            
            col1, col2 = st.columns(2)
            
            with col1:
                new_mandi = st.text_input("New Mandi Name")
                new_distance = st.number_input("Distance (km)", min_value=0.0, value=0.0, step=0.1)
                
                if st.button("➕ Add Mandi", type="primary"):
                    if new_mandi:
                        conn = get_connection()
                        cursor = conn.cursor()
                        try:
                            cursor.execute("""
                                INSERT INTO Settings_Mandi (mandi_name, distance_km) VALUES (?, ?)
                            """, (new_mandi.strip().upper(), new_distance))
                            conn.commit()
                            st.success("✅ Added!")
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error("❌ Mandi already exists")
                        finally:
                            conn.close()
            
            with col2:
                mandis_df = get_mandis()
                if not mandis_df.empty:
                    st.dataframe(mandis_df[['mandi_name', 'distance_km']], use_container_width=True, hide_index=True)
                    
                    del_mandi = st.selectbox("Delete Mandi", mandis_df['mandi_name'].tolist())
                    if st.button("🗑️ Delete Mandi"):
                        conn = get_connection()
                        conn.execute("DELETE FROM Settings_Mandi WHERE mandi_name = ?", (del_mandi,))
                        conn.commit()
                        conn.close()
                        st.rerun()
        
        # Godowns
        with settings_tab2:
            st.markdown("##### Godown Management")
            
            col1, col2 = st.columns(2)
            
            with col1:
                new_godown = st.text_input("New Godown Name")
                if st.button("➕ Add Godown", type="primary"):
                    if new_godown:
                        conn = get_connection()
                        cursor = conn.cursor()
                        try:
                            cursor.execute("INSERT INTO Settings_Godown (godown_name) VALUES (?)", 
                                          (new_godown.strip(),))
                            conn.commit()
                            st.success("✅ Added!")
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error("❌ Godown already exists")
                        finally:
                            conn.close()
            
            with col2:
                godowns_df = get_godowns()
                if not godowns_df.empty:
                    st.dataframe(godowns_df[['godown_name']], use_container_width=True, hide_index=True)
                    
                    del_godown = st.selectbox("Delete Godown", godowns_df['godown_name'].tolist())
                    if st.button("🗑️ Delete Godown"):
                        conn = get_connection()
                        conn.execute("DELETE FROM Settings_Godown WHERE godown_name = ?", (del_godown,))
                        conn.commit()
                        conn.close()
                        st.rerun()
        
        # Users
        with settings_tab3:
            st.markdown("##### User Management")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Add New User**")
                new_username = st.text_input("Username")
                new_password = st.text_input("Password", type="password")
                new_fullname = st.text_input("Full Name")
                new_phone = st.text_input("Phone")
                new_role = st.selectbox("Role", ["employee", "admin"])
                
                if st.button("➕ Add User", type="primary"):
                    if new_username and new_password:
                        conn = get_connection()
                        cursor = conn.cursor()
                        try:
                            cursor.execute("""
                                INSERT INTO Users (username, password_hash, role, full_name, phone)
                                VALUES (?, ?, ?, ?, ?)
                            """, (new_username, hash_password(new_password), new_role, new_fullname, new_phone))
                            conn.commit()
                            st.success("✅ User created!")
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error("❌ Username already exists")
                        finally:
                            conn.close()
                    else:
                        st.error("❌ Username and password required")
            
            with col2:
                st.markdown("**Existing Users**")
                users_df = get_users()
                if not users_df.empty:
                    display_users = users_df[['username', 'role', 'full_name', 'phone', 'is_active']].copy()
                    st.dataframe(display_users, use_container_width=True, hide_index=True)
                    
                    st.markdown("---")
                    
                    # Reset password
                    reset_user = st.selectbox("Reset Password For", users_df['username'].tolist())
                    new_pass = st.text_input("New Password", type="password", key="reset_pass")
                    if st.button("🔑 Reset Password"):
                        if new_pass:
                            conn = get_connection()
                            conn.execute("UPDATE Users SET password_hash = ? WHERE username = ?",
                                        (hash_password(new_pass), reset_user))
                            conn.commit()
                            conn.close()
                            st.success("✅ Password reset!")
                        else:
                            st.error("❌ Enter new password")
                    
                    # Delete user (except current user)
                    del_users = [u for u in users_df['username'].tolist() if u != st.session_state['username']]
                    if del_users:
                        del_user = st.selectbox("Delete User", del_users)
                        if st.button("🗑️ Delete User", type="secondary"):
                            conn = get_connection()
                            conn.execute("DELETE FROM Users WHERE username = ?", (del_user,))
                            conn.commit()
                            conn.close()
                            st.success("✅ Deleted!")
                            st.rerun()

# ============== MAIN APP ==============

def main():
    """Main application entry point"""
    
    # Initialize database
    init_database()
    
    # Check login state
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
