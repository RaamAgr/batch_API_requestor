import streamlit as st
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import json
import io

# --- Page Configuration ---
st.set_page_config(page_title="Batch API Runner", layout="wide", page_icon="🚀")

# --- Helper Functions ---

def get_session_with_retries(retries=3, backoff_factor=0.3):
    """
    Creates a requests session with automatic retry logic.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_data(session, base_url_pre, row_id, base_url_post):
    """
    Performs the API request for a single row and extracts specific JSON fields.
    """
    full_url = f"{base_url_pre}{row_id}{base_url_post}"
    
    result = {
        "id": row_id,
        "full_url": full_url,
        "status_code": 0,
        "main_disposition": None, # New Column
        "sub_disposition": None,  # New Column
        "response_json": "",
        "error": ""
    }

    try:
        response = session.get(full_url, timeout=150)
        result["status_code"] = response.status_code
        
        try:
            # Parse JSON
            data = response.json()
            result["response_json"] = json.dumps(data)
            
            # --- EXTRACTION LOGIC ---
            # Path: extraction -> extracted_data -> fields
            if "extraction" in data and "extracted_data" in data["extraction"]:
                extracted = data["extraction"]["extracted_data"]
                result["main_disposition"] = extracted.get("main_disposition")
                result["sub_disposition"] = extracted.get("sub_disposition")
            # ------------------------

        except ValueError:
            # If response is not JSON
            result["response_json"] = response.text
            
    except requests.exceptions.RequestException as e:
        result["error"] = str(e)
        result["status_code"] = -1 
        
    return result

def color_status(val):
    if val == 200:
        return 'background-color: #d4edda; color: #155724' 
    elif val == -1:
        return 'background-color: #f8d7da; color: #721c24' 
    else:
        return 'background-color: #fff3cd; color: #856404' 

def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# --- Main App Layout ---

st.title("🚀 Parallel Batch API Runner")
st.markdown("Run batch requests and extract **Disposition** data automatically.")

st.divider()

# 1. Configuration Section
col1, col2 = st.columns(2)

with col1:
    st.subheader("1. API Configuration")
    url_part_1 = st.text_input("Base URL (Before ID)", value="https://jsonplaceholder.typicode.com/todos/")
    url_part_2 = st.text_input("Query Params (After ID)", value="")

with col2:
    st.subheader("2. Run Settings")
    num_workers = st.slider("Number of Parallel Workers", min_value=1, max_value=20, value=5)
    uploaded_file = st.file_uploader("Upload Data (Must contain 'id' column)", type=["csv", "xlsx"])

st.divider()

# 3. Execution Section
if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        # Convert ID to string to avoid float issues (e.g. 101.0)
        if 'id' in df.columns:
            df['id'] = df['id'].astype(str)
    except Exception as e:
        st.error(f"Error reading file: {e}")
        st.stop()
    
    if 'id' not in df.columns:
        st.error("❌ The uploaded file must contain a column named 'id'.")
    else:
        st.success(f"✅ Loaded {len(df)} rows.")
        
        with st.expander("Preview Input Data"):
            st.dataframe(df.head())

        if st.button("Start Processing", type="primary"):
            
            results_list = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            session = get_session_with_retries()
            total_rows = len(df)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_id = {
                    executor.submit(fetch_data, session, url_part_1, row['id'], url_part_2): row 
                    for index, row in df.iterrows()
                }
                
                completed_count = 0
                
                for future in concurrent.futures.as_completed(future_to_id):
                    data = future.result()
                    original_row = future_to_id[future]
                    combined = {**original_row, **data}
                    results_list.append(combined)
                    
                    completed_count += 1
                    progress = completed_count / total_rows
                    progress_bar.progress(progress)
                    status_text.text(f"Processing: {completed_count}/{total_rows}...")

            progress_bar.progress(1.0)
            status_text.text("Processing Complete! 🎉")
            
            result_df = pd.DataFrame(results_list)
            
            # --- REORDER COLUMNS ---
            # Priority columns first
            priority_cols = ['id', 'status_code', 'main_disposition', 'sub_disposition', 'response_json', 'error']
            
            # Filter checks which of these actually exist in df (safeguard)
            existing_priority = [c for c in priority_cols if c in result_df.columns]
            remaining_cols = [c for c in result_df.columns if c not in existing_priority]
            
            result_df = result_df[existing_priority + remaining_cols]

            # --- Display Results ---
            st.subheader("Results Overview")
            
            # Show the main columns clearly in UI
            st.dataframe(
                result_df.style.map(color_status, subset=['status_code']),
                column_config={
                    "main_disposition": st.column_config.TextColumn("Main Disposition", help="Extracted from API"),
                    "sub_disposition": st.column_config.TextColumn("Sub Disposition", help="Extracted from API"),
                },
                use_container_width=True
            )

            # --- Download ---
            excel_data = convert_df_to_excel(result_df)
            
            st.download_button(
                label="📥 Download Results as Excel",
                data=excel_data,
                file_name="api_results_with_disposition.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
