"""
Rice Mill Procurement System
A professional Streamlit application for managing rice mill procurement operations
"""

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, date
from io import BytesIO

# Page configuration
st.set_page_config(
    page_title="Rice Mill Procurement System",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database initialization
DB_PATH = "rice_mill.db"

def init_database():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Settings_Mandi table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Settings_Mandi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandi_name TEXT UNIQUE NOT NULL,
            distance_km REAL NOT NULL
        )
    """)
    
    # Settings_Driver table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Settings_Driver (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_name TEXT UNIQUE NOT NULL,
            phone_number TEXT
        )
    """)
    
    # Vehicle_Registry table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Vehicle_Registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_number TEXT UNIQUE NOT NULL,
            owner_name TEXT,
            puc_expiry_date DATE,
            permit_number TEXT
        )
    """)
    
    # Arrivals table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Arrivals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            kms_year TEXT NOT NULL,
            ac_note_number TEXT,
            mandi_name TEXT NOT NULL,
            distance REAL NOT NULL,
            vehicle_number TEXT NOT NULL,
            driver_name TEXT NOT NULL,
            bag_count INTEGER NOT NULL,
            quantity_quintals REAL NOT NULL
        )
    """)
    
    # Master_Stock table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Master_Stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            kms_year TEXT NOT NULL,
            opening_balance REAL DEFAULT 0,
            received_today REAL DEFAULT 0,
            issued_milling REAL DEFAULT 0,
            closing_balance REAL DEFAULT 0,
            UNIQUE(date, kms_year)
        )
    """)
    
    conn.commit()
    conn.close()

# Database helper functions
def get_connection():
    """Create database connection"""
    return sqlite3.connect(DB_PATH)

def get_mandis():
    """Fetch all mandis"""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Settings_Mandi ORDER BY mandi_name", conn)
    conn.close()
    return df

def get_drivers():
    """Fetch all drivers"""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Settings_Driver ORDER BY driver_name", conn)
    conn.close()
    return df

def get_vehicles():
    """Fetch all vehicles"""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Vehicle_Registry ORDER BY vehicle_number", conn)
    conn.close()
    
    # Convert date strings to date objects
    if not df.empty and 'puc_expiry_date' in df.columns:
        df['puc_expiry_date'] = pd.to_datetime(df['puc_expiry_date'], errors='coerce').dt.date
    
    return df

def get_arrivals(kms_year):
    """Fetch arrivals for specific KMS year"""
    conn = get_connection()
    query = "SELECT * FROM Arrivals WHERE kms_year = ? ORDER BY date DESC, id DESC"
    df = pd.read_sql_query(query, conn, params=(kms_year,))
    conn.close()
    return df

def get_master_stock(kms_year):
    """Fetch master stock for specific KMS year"""
    conn = get_connection()
    query = "SELECT * FROM Master_Stock WHERE kms_year = ? ORDER BY date ASC"
    df = pd.read_sql_query(query, conn, params=(kms_year,))
    conn.close()
    return df

def update_master_stock(stock_date, kms_year):
    """Update master stock for a specific date"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Calculate received_today from Arrivals
        cursor.execute("""
            SELECT COALESCE(SUM(quantity_quintals), 0) 
            FROM Arrivals 
            WHERE date = ? AND kms_year = ?
        """, (stock_date, kms_year))
        received_today = cursor.fetchone()[0]
        
        # Get previous day's closing balance for opening balance
        cursor.execute("""
            SELECT closing_balance 
            FROM Master_Stock 
            WHERE kms_year = ? AND date < ? 
            ORDER BY date DESC 
            LIMIT 1
        """, (kms_year, stock_date))
        prev_result = cursor.fetchone()
        opening_balance = prev_result[0] if prev_result else 0
        
        # Get current issued_milling (preserve user edits)
        cursor.execute("""
            SELECT issued_milling 
            FROM Master_Stock 
            WHERE date = ? AND kms_year = ?
        """, (stock_date, kms_year))
        issued_result = cursor.fetchone()
        issued_milling = issued_result[0] if issued_result else 0
        
        # Calculate closing balance
        closing_balance = opening_balance + received_today - issued_milling
        
        # Insert or update record
        cursor.execute("""
            INSERT INTO Master_Stock (date, kms_year, opening_balance, received_today, issued_milling, closing_balance)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, kms_year) 
            DO UPDATE SET 
                opening_balance = excluded.opening_balance,
                received_today = excluded.received_today,
                closing_balance = excluded.closing_balance
        """, (stock_date, kms_year, opening_balance, received_today, issued_milling, closing_balance))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Error updating master stock: {e}")
        return False
    finally:
        conn.close()

def recalculate_all_stock(kms_year):
    """Recalculate all stock balances for the year"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get all dates with arrivals or stock entries
        cursor.execute("""
            SELECT DISTINCT date FROM (
                SELECT date FROM Arrivals WHERE kms_year = ?
                UNION
                SELECT date FROM Master_Stock WHERE kms_year = ?
            ) ORDER BY date ASC
        """, (kms_year, kms_year))
        
        dates = [row[0] for row in cursor.fetchall()]
        
        for stock_date in dates:
            update_master_stock(stock_date, kms_year)
        
        return True
    except Exception as e:
        st.error(f"Error recalculating stock: {e}")
        return False
    finally:
        conn.close()

def to_excel(df):
    """Convert dataframe to Excel bytes"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

# Initialize database
init_database()

# App Title
st.title("🌾 Rice Mill Procurement System")

# Sidebar - KMS Year Selection
st.sidebar.header("📅 Filter Settings")
current_year = datetime.now().year
year_options = [f"{y}-{str(y+1)[-2:]}" for y in range(current_year-2, current_year+3)]
kms_year = st.sidebar.selectbox("Select KMS Year", year_options, index=2)

st.sidebar.markdown("---")
st.sidebar.info(f"**Active Year:** {kms_year}")

# Main Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "🌾 New Arrival Entry",
    "📈 Master Stock Register", 
    "🚛 Vehicle Registry",
    "⚙️ Settings"
])

# TAB 1: New Arrival Entry
with tab1:
    st.header("📝 New Arrival Entry")
    
    col1, col2 = st.columns(2)
    
    with col1:
        entry_date = st.date_input("Date", value=date.today(), key="arrival_date")
        ac_note = st.text_input("AC Note Number (Optional)", key="ac_note")
        
        # Mandi selection
        mandis_df = get_mandis()
        if not mandis_df.empty:
            mandi_options = mandis_df['mandi_name'].tolist()
            selected_mandi = st.selectbox("Select Mandi", mandi_options, key="mandi_select")
            
            # Auto-fill distance
            mandi_distance = mandis_df[mandis_df['mandi_name'] == selected_mandi]['distance_km'].values[0]
            distance = st.number_input("Distance (km)", value=float(mandi_distance), min_value=0.0, step=0.1, key="distance")
        else:
            st.warning("⚠️ No mandis configured. Please add mandis in Settings tab.")
            selected_mandi = None
            distance = st.number_input("Distance (km)", value=0.0, min_value=0.0, step=0.1, key="distance")
    
    with col2:
        # Vehicle selection
        vehicles_df = get_vehicles()
        if not vehicles_df.empty:
            vehicle_options = vehicles_df['vehicle_number'].tolist()
            selected_vehicle = st.selectbox("Select Vehicle", vehicle_options, key="vehicle_select")
        else:
            st.warning("⚠️ No vehicles registered. Please add vehicles in Vehicle Registry tab.")
            selected_vehicle = None
        
        # Driver selection
        drivers_df = get_drivers()
        if not drivers_df.empty:
            driver_options = drivers_df['driver_name'].tolist()
            selected_driver = st.selectbox("Select Driver", driver_options, key="driver_select")
        else:
            st.warning("⚠️ No drivers configured. Please add drivers in Settings tab.")
            selected_driver = None
        
        bag_count = st.number_input("Bag Count", min_value=0, value=0, step=1, key="bag_count")
        quantity = st.number_input("Quantity (Quintals)", min_value=0.0, value=0.0, step=0.1, key="quantity")
    
    st.markdown("---")
    
    if st.button("💾 Submit Arrival", type="primary", use_container_width=True):
        if not selected_mandi:
            st.error("❌ Please add a mandi in Settings first.")
        elif not selected_vehicle:
            st.error("❌ Please add a vehicle in Vehicle Registry first.")
        elif not selected_driver:
            st.error("❌ Please add a driver in Settings first.")
        elif bag_count <= 0:
            st.error("❌ Bag count must be greater than 0.")
        elif quantity <= 0:
            st.error("❌ Quantity must be greater than 0.")
        else:
            conn = get_connection()
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    INSERT INTO Arrivals (date, kms_year, ac_note_number, mandi_name, distance, 
                                        vehicle_number, driver_name, bag_count, quantity_quintals)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (entry_date, kms_year, ac_note, selected_mandi, distance, 
                      selected_vehicle, selected_driver, bag_count, quantity))
                
                conn.commit()
                
                # Update master stock
                update_master_stock(str(entry_date), kms_year)
                
                st.success("✅ Arrival entry saved successfully!")
                st.balloons()
                
            except Exception as e:
                conn.rollback()
                st.error(f"❌ Error saving arrival: {e}")
            finally:
                conn.close()
    
    # Display recent arrivals
    st.markdown("---")
    st.subheader("📋 Recent Arrivals")
    
    arrivals_df = get_arrivals(kms_year)
    if not arrivals_df.empty:
        # Format display - keep id for reference but don't show it
        display_df = arrivals_df[['date', 'ac_note_number', 'mandi_name', 'distance', 'vehicle_number', 
                                   'driver_name', 'bag_count', 'quantity_quintals']].copy()
        display_df.columns = ['Date', 'AC Note', 'Mandi', 'Distance (km)', 'Vehicle', 'Driver', 'Bags', 'Quantity (Q)']
        
        # Editable table
        edited_arrivals = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            disabled=['Date'],
            column_config={
                "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                "AC Note": st.column_config.TextColumn("AC Note"),
                "Mandi": st.column_config.TextColumn("Mandi"),
                "Distance (km)": st.column_config.NumberColumn("Distance (km)", format="%.1f"),
                "Vehicle": st.column_config.TextColumn("Vehicle"),
                "Driver": st.column_config.TextColumn("Driver"),
                "Bags": st.column_config.NumberColumn("Bags", format="%d"),
                "Quantity (Q)": st.column_config.NumberColumn("Quantity (Q)", format="%.2f"),
            },
            key="arrivals_editor"
        )
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            if st.button("💾 Save Changes", type="primary", use_container_width=True, key="save_arrivals"):
                conn = get_connection()
                cursor = conn.cursor()
                
                try:
                    for idx, row in edited_arrivals.iterrows():
                        arrival_id = arrivals_df.iloc[idx]['id']
                        arrival_date = arrivals_df.iloc[idx]['date']
                        
                        cursor.execute("""
                            UPDATE Arrivals 
                            SET ac_note_number = ?, mandi_name = ?, distance = ?, 
                                vehicle_number = ?, driver_name = ?, bag_count = ?, quantity_quintals = ?
                            WHERE id = ?
                        """, (row['AC Note'], row['Mandi'], row['Distance (km)'], 
                              row['Vehicle'], row['Driver'], int(row['Bags']), row['Quantity (Q)'], arrival_id))
                        
                        # Update master stock for affected date
                        update_master_stock(arrival_date, kms_year)
                    
                    conn.commit()
                    st.success("✅ Arrival records updated successfully!")
                    st.rerun()
                    
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ Error updating arrivals: {e}")
                finally:
                    conn.close()
        
        with col2:
            # Delete arrival - create a simple list of descriptions
            if len(arrivals_df) > 0:
                # Create display options
                arrival_display_list = []
                for idx in range(len(arrivals_df)):
                    row = arrivals_df.iloc[idx]
                    display_text = f"{row['date']} | {row['vehicle_number']} | {row['quantity_quintals']}Q"
                    arrival_display_list.append(display_text)
                
                selected_arrival = st.selectbox(
                    "Select arrival to delete", 
                    arrival_display_list,
                    key="delete_arrival_select"
                )
                
                if st.button("🗑️ Delete Arrival", type="secondary", use_container_width=True):
                    # Find the index of selected arrival
                    selected_idx = arrival_display_list.index(selected_arrival)
                    
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        arrival_id = arrivals_df.iloc[selected_idx]['id']
                        arrival_date = arrivals_df.iloc[selected_idx]['date']
                        
                        cursor.execute("DELETE FROM Arrivals WHERE id = ?", (arrival_id,))
                        conn.commit()
                        
                        # Update master stock for affected date
                        update_master_stock(arrival_date, kms_year)
                        
                        st.success("✅ Arrival deleted successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error deleting arrival: {e}")
                    finally:
                        conn.close()
        
        with col3:
            # Download button
            excel_data = to_excel(display_df)
            st.download_button(
                label="📥 Excel",
                data=excel_data,
                file_name=f"arrivals_{kms_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    else:
        st.info("ℹ️ No arrival records for this KMS year.")

# TAB 2: Master Stock Register
with tab2:
    st.header("📊 Master Stock Register")
    
    # Recalculate button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Recalculate Stock", use_container_width=True):
            if recalculate_all_stock(kms_year):
                st.success("✅ Stock recalculated successfully!")
                st.rerun()
    
    stock_df = get_master_stock(kms_year)
    
    if not stock_df.empty:
        # Create editable dataframe
        st.subheader("Stock Register")
        
        # Format for display
        display_stock = stock_df[['date', 'opening_balance', 'received_today', 
                                   'issued_milling', 'closing_balance']].copy()
        display_stock.columns = ['Date', 'Opening Balance', 'Received Today', 
                                'Issued to Milling', 'Closing Balance']
        
        # Add Total column
        display_stock.insert(3, 'Total', display_stock['Opening Balance'] + display_stock['Received Today'])
        
        # Display as editable table
        edited_df = st.data_editor(
            display_stock,
            use_container_width=True,
            hide_index=True,
            disabled=['Date', 'Opening Balance', 'Received Today', 'Total', 'Closing Balance'],
            column_config={
                "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                "Opening Balance": st.column_config.NumberColumn("Opening Balance", format="%.2f"),
                "Received Today": st.column_config.NumberColumn("Received Today", format="%.2f"),
                "Total": st.column_config.NumberColumn("Total", format="%.2f"),
                "Issued to Milling": st.column_config.NumberColumn("Issued to Milling", format="%.2f"),
                "Closing Balance": st.column_config.NumberColumn("Closing Balance", format="%.2f"),
            }
        )
        
        # Save changes button
        if st.button("💾 Save Changes", type="primary"):
            conn = get_connection()
            cursor = conn.cursor()
            
            try:
                for idx, row in edited_df.iterrows():
                    stock_date = stock_df.iloc[idx]['date']
                    issued = row['Issued to Milling']
                    
                    cursor.execute("""
                        UPDATE Master_Stock 
                        SET issued_milling = ?
                        WHERE date = ? AND kms_year = ?
                    """, (issued, stock_date, kms_year))
                
                conn.commit()
                
                # Recalculate closing balances
                recalculate_all_stock(kms_year)
                
                st.success("✅ Changes saved successfully!")
                st.rerun()
                
            except Exception as e:
                conn.rollback()
                st.error(f"❌ Error saving changes: {e}")
            finally:
                conn.close()
        
        # Summary metrics
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_received = display_stock['Received Today'].sum()
            st.metric("Total Received", f"{total_received:.2f} Q")
        
        with col2:
            total_issued = display_stock['Issued to Milling'].sum()
            st.metric("Total Issued", f"{total_issued:.2f} Q")
        
        with col3:
            current_stock = display_stock['Closing Balance'].iloc[-1]
            st.metric("Current Stock", f"{current_stock:.2f} Q")
        
        with col4:
            opening_stock = display_stock['Opening Balance'].iloc[0]
            st.metric("Opening Stock", f"{opening_stock:.2f} Q")
        
        # Download button
        st.markdown("---")
        excel_data = to_excel(display_stock)
        st.download_button(
            label="📥 Download Stock Register (Excel)",
            data=excel_data,
            file_name=f"stock_register_{kms_year}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("ℹ️ No stock records for this KMS year. Stock will be created automatically when you add arrivals.")
        
        # Manual opening balance entry for first day
        st.markdown("---")
        st.subheader("🔧 Initialize Opening Stock")
        
        col1, col2 = st.columns(2)
        with col1:
            init_date = st.date_input("Starting Date", value=date.today())
        with col2:
            opening_bal = st.number_input("Opening Balance (Quintals)", min_value=0.0, value=0.0, step=0.1)
        
        if st.button("✅ Set Opening Balance", type="primary"):
            conn = get_connection()
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    INSERT INTO Master_Stock (date, kms_year, opening_balance, received_today, 
                                            issued_milling, closing_balance)
                    VALUES (?, ?, ?, 0, 0, ?)
                """, (init_date, kms_year, opening_bal, opening_bal))
                
                conn.commit()
                st.success("✅ Opening balance set successfully!")
                st.rerun()
                
            except sqlite3.IntegrityError:
                st.error("❌ Stock entry already exists for this date.")
            except Exception as e:
                conn.rollback()
                st.error(f"❌ Error: {e}")
            finally:
                conn.close()

# TAB 3: Vehicle Registry
with tab3:
    st.header("🚛 Vehicle Registry")
    
    # Add new vehicle section
    with st.expander("➕ Add New Vehicle", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            new_vehicle_number = st.text_input("Vehicle Number", key="new_vehicle_num")
            new_owner_name = st.text_input("Owner Name", key="new_owner")
        
        with col2:
            new_puc_expiry = st.date_input("PUC Expiry Date", key="new_puc")
            new_permit_number = st.text_input("Permit Number", key="new_permit")
        
        if st.button("➕ Add Vehicle", type="primary", use_container_width=True):
            if not new_vehicle_number:
                st.error("❌ Vehicle number is required.")
            else:
                conn = get_connection()
                cursor = conn.cursor()
                
                try:
                    cursor.execute("""
                        INSERT INTO Vehicle_Registry (vehicle_number, owner_name, puc_expiry_date, permit_number)
                        VALUES (?, ?, ?, ?)
                    """, (new_vehicle_number.strip().upper(), new_owner_name, new_puc_expiry, new_permit_number))
                    
                    conn.commit()
                    st.success("✅ Vehicle added successfully!")
                    st.rerun()
                    
                except sqlite3.IntegrityError:
                    st.error("❌ Vehicle number already exists.")
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ Error: {e}")
                finally:
                    conn.close()
    
    # Display existing vehicles
    st.markdown("---")
    st.subheader("📋 Registered Vehicles")
    
    vehicles_df = get_vehicles()
    
    if not vehicles_df.empty:
        # Create display dataframe without id column
        display_vehicles = vehicles_df[['vehicle_number', 'owner_name', 'puc_expiry_date', 'permit_number']].copy()
        
        # Display as editable table
        edited_vehicles = st.data_editor(
            display_vehicles,
            use_container_width=True,
            hide_index=True,
            disabled=['vehicle_number'],
            column_config={
                "vehicle_number": st.column_config.TextColumn("Vehicle Number"),
                "owner_name": st.column_config.TextColumn("Owner Name"),
                "puc_expiry_date": st.column_config.DateColumn("PUC Expiry", format="DD/MM/YYYY"),
                "permit_number": st.column_config.TextColumn("Permit Number"),
            },
            key="vehicle_editor"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Save Changes", type="primary", use_container_width=True, key="save_vehicles"):
                conn = get_connection()
                cursor = conn.cursor()
                
                try:
                    for idx, row in edited_vehicles.iterrows():
                        vehicle_id = vehicles_df.iloc[idx]['id']
                        cursor.execute("""
                            UPDATE Vehicle_Registry 
                            SET owner_name = ?, puc_expiry_date = ?, permit_number = ?
                            WHERE id = ?
                        """, (row['owner_name'], row['puc_expiry_date'], row['permit_number'], vehicle_id))
                    
                    conn.commit()
                    st.success("✅ Changes saved successfully!")
                    st.rerun()
                    
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ Error saving changes: {e}")
                finally:
                    conn.close()
        
        with col2:
            # Delete vehicle
            vehicle_to_delete = st.selectbox("Select vehicle to delete", 
                                            vehicles_df['vehicle_number'].tolist(),
                                            key="delete_vehicle_select")
            
            if st.button("🗑️ Delete Vehicle", type="secondary", use_container_width=True):
                conn = get_connection()
                cursor = conn.cursor()
                
                try:
                    cursor.execute("DELETE FROM Vehicle_Registry WHERE vehicle_number = ?", 
                                 (vehicle_to_delete,))
                    conn.commit()
                    st.success("✅ Vehicle deleted successfully!")
                    st.rerun()
                    
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ Error deleting vehicle: {e}")
                finally:
                    conn.close()
    else:
        st.info("ℹ️ No vehicles registered yet. Add your first vehicle above.")

# TAB 4: Settings
with tab4:
    st.header("⚙️ System Settings")
    
    settings_tab1, settings_tab2 = st.tabs(["🏪 Mandis", "👤 Drivers"])
    
    # Settings Tab A: Mandis
    with settings_tab1:
        st.subheader("Mandi Management")
        
        # Add new mandi
        with st.expander("➕ Add New Mandi", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                new_mandi_name = st.text_input("Mandi Name", key="new_mandi_name")
            with col2:
                new_mandi_distance = st.number_input("Distance (km)", min_value=0.0, value=0.0, 
                                                    step=0.1, key="new_mandi_dist")
            
            if st.button("➕ Add Mandi", type="primary", use_container_width=True):
                if not new_mandi_name:
                    st.error("❌ Mandi name is required.")
                else:
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute("""
                            INSERT INTO Settings_Mandi (mandi_name, distance_km)
                            VALUES (?, ?)
                        """, (new_mandi_name.strip(), new_mandi_distance))
                        
                        conn.commit()
                        st.success("✅ Mandi added successfully!")
                        st.rerun()
                        
                    except sqlite3.IntegrityError:
                        st.error("❌ Mandi name already exists.")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error: {e}")
                    finally:
                        conn.close()
        
        # Display existing mandis
        st.markdown("---")
        mandis_df = get_mandis()
        
        if not mandis_df.empty:
            # Create display dataframe without id column
            display_mandis = mandis_df[['mandi_name', 'distance_km']].copy()
            
            edited_mandis = st.data_editor(
                display_mandis,
                use_container_width=True,
                hide_index=True,
                disabled=['mandi_name'],
                column_config={
                    "mandi_name": st.column_config.TextColumn("Mandi Name"),
                    "distance_km": st.column_config.NumberColumn("Distance (km)", format="%.1f"),
                },
                key="mandi_editor"
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("💾 Save Mandi Changes", type="primary", use_container_width=True):
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        for idx, row in edited_mandis.iterrows():
                            mandi_id = mandis_df.iloc[idx]['id']
                            cursor.execute("""
                                UPDATE Settings_Mandi 
                                SET distance_km = ?
                                WHERE id = ?
                            """, (row['distance_km'], mandi_id))
                        
                        conn.commit()
                        st.success("✅ Changes saved successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error saving changes: {e}")
                    finally:
                        conn.close()
            
            with col2:
                mandi_to_delete = st.selectbox("Select mandi to delete", 
                                              mandis_df['mandi_name'].tolist(),
                                              key="delete_mandi_select")
                
                if st.button("🗑️ Delete Mandi", type="secondary", use_container_width=True):
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute("DELETE FROM Settings_Mandi WHERE mandi_name = ?", 
                                     (mandi_to_delete,))
                        conn.commit()
                        st.success("✅ Mandi deleted successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error deleting mandi: {e}")
                    finally:
                        conn.close()
        else:
            st.info("ℹ️ No mandis configured yet. Add your first mandi above.")
    
    # Settings Tab B: Drivers
    with settings_tab2:
        st.subheader("Driver Management")
        
        # Add new driver
        with st.expander("➕ Add New Driver", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                new_driver_name = st.text_input("Driver Name", key="new_driver_name")
            with col2:
                new_phone_number = st.text_input("Phone Number", key="new_phone_num")
            
            if st.button("➕ Add Driver", type="primary", use_container_width=True):
                if not new_driver_name:
                    st.error("❌ Driver name is required.")
                else:
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute("""
                            INSERT INTO Settings_Driver (driver_name, phone_number)
                            VALUES (?, ?)
                        """, (new_driver_name.strip(), new_phone_number))
                        
                        conn.commit()
                        st.success("✅ Driver added successfully!")
                        st.rerun()
                        
                    except sqlite3.IntegrityError:
                        st.error("❌ Driver name already exists.")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error: {e}")
                    finally:
                        conn.close()
        
        # Display existing drivers
        st.markdown("---")
        drivers_df = get_drivers()
        
        if not drivers_df.empty:
            # Create display dataframe without id column
            display_drivers = drivers_df[['driver_name', 'phone_number']].copy()
            
            edited_drivers = st.data_editor(
                display_drivers,
                use_container_width=True,
                hide_index=True,
                disabled=['driver_name'],
                column_config={
                    "driver_name": st.column_config.TextColumn("Driver Name"),
                    "phone_number": st.column_config.TextColumn("Phone Number"),
                },
                key="driver_editor"
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("💾 Save Driver Changes", type="primary", use_container_width=True):
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        for idx, row in edited_drivers.iterrows():
                            driver_id = drivers_df.iloc[idx]['id']
                            cursor.execute("""
                                UPDATE Settings_Driver 
                                SET phone_number = ?
                                WHERE id = ?
                            """, (row['phone_number'], driver_id))
                        
                        conn.commit()
                        st.success("✅ Changes saved successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error saving changes: {e}")
                    finally:
                        conn.close()
            
            with col2:
                driver_to_delete = st.selectbox("Select driver to delete", 
                                               drivers_df['driver_name'].tolist(),
                                               key="delete_driver_select")
                
                if st.button("🗑️ Delete Driver", type="secondary", use_container_width=True):
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute("DELETE FROM Settings_Driver WHERE driver_name = ?", 
                                     (driver_to_delete,))
                        conn.commit()
                        st.success("✅ Driver deleted successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Error deleting driver: {e}")
                    finally:
                        conn.close()
        else:
            st.info("ℹ️ No drivers configured yet. Add your first driver above.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p>🌾 Rice Mill Procurement System v1.0 | Built with Streamlit</p>
    </div>
    """,
    unsafe_allow_html=True
)
