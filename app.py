import streamlit as st
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import time
import json

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
    Performs the API request for a single row.
    Returns a dictionary with status, response, and the original ID.
    """
    # Construct the full URL
    # Structure: [Pre URL] + [ID] + [Post URL/Params]
    full_url = f"{base_url_pre}{row_id}{base_url_post}"
    
    result = {
        "id": row_id,
        "full_url": full_url,
        "status_code": 0,
        "response_json": "",
        "error": ""
    }

    try:
        response = session.get(full_url, timeout=10)
        result["status_code"] = response.status_code
        
        # Try to parse JSON, fall back to text if not JSON
        try:
            result["response_json"] = json.dumps(response.json())
        except ValueError:
            result["response_json"] = response.text
            
    except requests.exceptions.RequestException as e:
        result["error"] = str(e)
        result["status_code"] = -1 # Custom code for connection error
        
    return result

def color_status(val):
    """
    Color coding for the dataframe status column.
    """
    if val == 200:
        return 'background-color: #d4edda; color: #155724' # Green
    elif val == -1:
        return 'background-color: #f8d7da; color: #721c24' # Red (Network Error)
    else:
        return 'background-color: #fff3cd; color: #856404' # Yellow (Other HTTP codes)

# --- Main App Layout ---

st.title("🚀 Parallel Batch API Runner")
st.markdown("Run batch requests against your API by injecting IDs from a CSV.")

st.divider()

# 1. Configuration Section
col1, col2 = st.columns(2)

with col1:
    st.subheader("1. API Configuration")
    st.info("URL Format: `Base Part` + `{{id}}` + `Query Params`")
    url_part_1 = st.text_input("Base URL (Before ID)", value="https://jsonplaceholder.typicode.com/todos/")
    url_part_2 = st.text_input("Query Params (After ID)", value="?source=batch_app")

with col2:
    st.subheader("2. Run Settings")
    num_workers = st.slider("Number of Parallel Workers", min_value=1, max_value=20, value=5)
    uploaded_file = st.file_uploader("Upload CSV (Must contain 'id' column)", type=["csv"])

st.divider()

# 3. Execution Section
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    
    # Validation
    if 'id' not in df.columns:
        st.error("❌ The uploaded CSV must contain a column named 'id'.")
    else:
        st.success(f"✅ Loaded {len(df)} rows ready for processing.")
        
        # Preview
        with st.expander("Preview Input Data"):
            st.dataframe(df.head())

        if st.button("Start Processing", type="primary"):
            
            results_list = []
            
            # Progress Bar & Status Text
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            session = get_session_with_retries()
            total_rows = len(df)
            
            # ThreadPool Execution
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Map futures to IDs
                future_to_id = {
                    executor.submit(fetch_data, session, url_part_1, row['id'], url_part_2): row 
                    for index, row in df.iterrows()
                }
                
                completed_count = 0
                
                # As requests complete
                for future in concurrent.futures.as_completed(future_to_id):
                    data = future.result()
                    
                    # Merge original row data with result
                    original_row = future_to_id[future]
                    # We create a combined dict
                    combined = {**original_row, **data}
                    results_list.append(combined)
                    
                    completed_count += 1
                    progress = completed_count / total_rows
                    progress_bar.progress(progress)
                    status_text.text(f"Processing: {completed_count}/{total_rows} requests completed...")

            # Processing Complete
            progress_bar.progress(1.0)
            status_text.text("Processing Complete! 🎉")
            
            # Create Result Dataframe
            result_df = pd.DataFrame(results_list)
            
            # Reorder columns to put interesting stuff first
            cols = ['id', 'status_code', 'response_json', 'full_url', 'error']
            # Add any other columns from original csv that aren't in cols
            remaining_cols = [c for c in result_df.columns if c not in cols]
            result_df = result_df[cols + remaining_cols]

            # --- Display Results ---
            st.subheader("Results")
            
            # Apply styling
            st.dataframe(
                result_df.style.map(color_status, subset=['status_code']),
                use_container_width=True
            )

            # --- Download ---
            csv = result_df.to_csv(index=False).encode('utf-8')
            
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv,
                file_name="api_results.csv",
                mime="text/csv",
            )
