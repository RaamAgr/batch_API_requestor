import streamlit as st
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import json
import io
import time

# --- Page Configuration ---
st.set_page_config(page_title="Batch API Runner & Viewer", layout="wide", page_icon="🚀")

# --- Helper Functions ---

def get_session_with_retries(retries=0, backoff_factor=0.3):
    """
    Creates a requests session.
    Retries are set to 0 as requested.
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

def parse_extraction_data(json_input):
    """
    Parses specific fields from the JSON string or dict.
    Returns a dictionary with mobile, dispositions, and time.
    Handles None/Null/Empty inputs gracefully.
    """
    # Initialize default values
    parsed = {
        "mobile_number": None,
        "main_disposition": None,
        "sub_disposition": None,
        "updated_at": None
    }
    
    data = {}
    
    # Safe JSON Loading Logic
    if isinstance(json_input, str):
        try:
            # Check for empty strings or 'nan' strings
            if not json_input.strip() or json_input.lower() == 'nan':
                return parsed
                
            loaded_data = json.loads(json_input)
            
            # CRITICAL FIX: Ensure loaded data is actually a dictionary
            # json.loads("null") returns None, which causes AttributeError later
            if isinstance(loaded_data, dict):
                data = loaded_data
            else:
                return parsed
        except:
            return parsed # Return empty if parsing fails
    elif isinstance(json_input, dict):
        data = json_input
    else:
        # Handles NaN (float) or NoneType
        return parsed

    # 1. Extract Mobile Number
    parsed["mobile_number"] = data.get("mobile_number") 

    # 2. Extract Dispositions
    extraction_block = data.get("extraction", {})
    if extraction_block and isinstance(extraction_block, dict):
        extracted_data = extraction_block.get("extracted_data", {})
        if extracted_data and isinstance(extracted_data, dict):
            parsed["main_disposition"] = extracted_data.get("main_disposition")
            parsed["sub_disposition"] = extracted_data.get("sub_disposition")

    # 3. Extract Time
    parsed["updated_at"] = data.get("conversation_time")
    
    return parsed

def fetch_data(session, base_url_pre, row_id, base_url_post):
    """
    Performs the API request for a single row.
    """
    full_url = f"{base_url_pre}{row_id}{base_url_post}"
    
    result = {
        "id": row_id,
        "full_url": full_url,
        "status_code": 0,
        "response_json": "",
        "error": ""
    }

    try:
        # Timeout set to 200 seconds
        response = session.get(full_url, timeout=200)
        result["status_code"] = response.status_code
        
        try:
            # We store the raw JSON string
            data = response.json()
            result["response_json"] = json.dumps(data)
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

def convert_df_to_excel(df):
    """
    Converts a dataframe to an in-memory Excel file.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# --- Main App Layout ---

st.title("🚀 API Batch Runner & Data Viewer")

st.markdown("""
- **Runner Mode:** Upload a file with an `id` column to call the API.
- **Viewer Mode:** Upload a file with a `response_json` column to view parsed results instantly.
""")
st.divider()

uploaded_file = st.file_uploader("Upload Excel/CSV", type=["csv", "xlsx"])

if uploaded_file is not None:
    # 1. Load the Data
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        # Ensure ID is string if it exists
        if 'id' in df.columns:
            df['id'] = df['id'].astype(str)
            
    except Exception as e:
        st.error(f"Error reading file: {e}")
        st.stop()

    st.write(f"**Loaded File:** {uploaded_file.name} ({len(df)} rows)")

    # ---------------------------------------------------------
    # MODE A: VIEWER MODE (If 'response_json' exists)
    # ---------------------------------------------------------
    if "response_json" in df.columns:
        st.info("📊 **View Mode Detected:** Parsing 'response_json' column...")
        
        # Parse the JSON column
        # We use apply to run the parser on every row
        parsed_df = df["response_json"].apply(parse_extraction_data).apply(pd.Series)
        
        # Combine original data with parsed data
        # We drop columns from parsed_df that might already exist in df to avoid duplicates
        parsed_df = parsed_df[[c for c in parsed_df.columns if c not in df.columns]]
        display_df = pd.concat([df, parsed_df], axis=1)

        # Reorder Columns for better UI
        # Priority: Mobile, Dispositions, Time, then ID, then others
        priority_cols = ["id", "mobile_number", "main_disposition", "sub_disposition", "updated_at", "status_code"]
        
        # Check which priority columns actually exist
        existing_priority = [c for c in priority_cols if c in display_df.columns]
        other_cols = [c for c in display_df.columns if c not in existing_priority]
        
        final_view_df = display_df[existing_priority + other_cols]

        st.dataframe(
            final_view_df.style.map(color_status, subset=['status_code']) if 'status_code' in final_view_df.columns else final_view_df,
            use_container_width=True
        )

        # Download Button
        st.download_button(
            label="📥 Download Parsed Excel",
            data=convert_df_to_excel(final_view_df),
            file_name="parsed_api_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # ---------------------------------------------------------
    # MODE B: RUNNER MODE (If 'response_json' is missing)
    # ---------------------------------------------------------
    else:
        st.warning("⚙️ **Runner Mode:** No 'response_json' found. Configure API settings below.")
        
        if 'id' not in df.columns:
            st.error("❌ Column 'id' is required for Runner Mode.")
        else:
            # Inputs
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("1. API Settings")
                url_part_1 = st.text_input("Base URL (Before ID)", value="https://abc.com/something/")
                url_part_2 = st.text_input("Query Params (After ID)", value="?params=1")
            
            with col2:
                st.subheader("2. Worker Settings")
                num_workers = st.slider("Parallel Workers", 1, 20, 5)

            st.divider()

            if st.button("Start Batch Processing", type="primary"):
                results_list = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Setup session with 0 retries
                session = get_session_with_retries(retries=0)
                total_rows = len(df)
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                    # Submit all tasks
                    future_to_row = {
                        executor.submit(fetch_data, session, url_part_1, row['id'], url_part_2): row 
                        for index, row in df.iterrows()
                    }
                    
                    completed_count = 0
                    
                    for future in concurrent.futures.as_completed(future_to_row):
                        result_data = future.result()
                        original_row = future_to_row[future]
                        
                        # 1. Parse immediately so we can show it in the UI even during the run
                        parsed_fields = parse_extraction_data(result_data["response_json"])
                        
                        # 2. Merge everything: Original Row + API Result + Parsed Fields
                        # Note: We convert original_row to dict
                        combined = {**original_row.to_dict(), **result_data, **parsed_fields}
                        results_list.append(combined)
                        
                        completed_count += 1
                        progress = completed_count / total_rows
                        progress_bar.progress(progress)
                        status_text.text(f"Processing: {completed_count}/{total_rows} completed...")

                progress_bar.progress(1.0)
                status_text.success("Processing Complete! 🎉")
                
                # Create Final DataFrame
                result_df = pd.DataFrame(results_list)
                
                # Reorder Logic
                priority_cols = ["id", "status_code", "main_disposition", "sub_disposition", "updated_at", "full_url", "error", "response_json"]
                existing_priority = [c for c in priority_cols if c in result_df.columns]
                remaining_cols = [c for c in result_df.columns if c not in existing_priority]
                
                result_df = result_df[existing_priority + remaining_cols]

                # Display with Styling
                st.subheader("Results")
                st.dataframe(
                    result_df.style.map(color_status, subset=['status_code']),
                    use_container_width=True
                )

                # Download
                st.download_button(
                    label="📥 Download Results as Excel",
                    data=convert_df_to_excel(result_df),
                    file_name="api_results_final.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
