import streamlit as st
import pandas as pd
import requests
import json
import time
from io import BytesIO

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="API Batch Caller",
    page_icon="⚡",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Dark gradient background */
.stApp {
    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    min-height: 100vh;
}

/* Glass card */
.glass-card {
    background: rgba(255, 255, 255, 0.07);
    border: 1px solid rgba(255, 255, 255, 0.12);
    border-radius: 16px;
    padding: 1.5rem 2rem;
    backdrop-filter: blur(12px);
    margin-bottom: 1.5rem;
}

/* Section header */
.section-title {
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #a78bfa;
    margin-bottom: 0.5rem;
}

/* Result row styling */
.result-row {
    background: rgba(255,255,255,0.05);
    border-radius: 10px;
    padding: 0.8rem 1.2rem;
    margin-bottom: 0.6rem;
    border-left: 3px solid #6d28d9;
    animation: fadeIn 0.3s ease;
}
.result-row.success { border-left-color: #10b981; }
.result-row.error   { border-left-color: #ef4444; }

@keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* Badge */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.72rem;
    font-weight: 600;
    margin-left: 8px;
}
.badge-success { background: #065f46; color: #6ee7b7; }
.badge-error   { background: #7f1d1d; color: #fca5a5; }

/* Inputs */
.stTextInput input, .stNumberInput input {
    background: rgba(255,255,255,0.08) !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    border-radius: 10px !important;
    color: #e2e8f0 !important;
}

/* Button */
.stButton > button {
    background: linear-gradient(90deg, #7c3aed, #4f46e5) !important;
    color: white !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 0.55rem 2rem !important;
    font-weight: 600 !important;
    transition: transform 0.15s, box-shadow 0.15s !important;
}
.stButton > button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 20px rgba(124,58,237,0.4) !important;
}

/* Expander */
details summary {
    color: #c4b5fd !important;
    font-size: 0.85rem;
}

/* File uploader */
.stFileUploader {
    background: rgba(255,255,255,0.05) !important;
    border-radius: 12px !important;
}

/* Selectbox */
.stSelectbox > div > div {
    background: rgba(255,255,255,0.08) !important;
    border-radius: 10px !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
}
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center; padding: 2rem 0 1rem;'>
    <h1 style='font-size:2.6rem; font-weight:700;
               background: linear-gradient(90deg,#a78bfa,#60a5fa);
               -webkit-background-clip:text; -webkit-text-fill-color:transparent;
               margin-bottom:0.3rem;'>
        ⚡ API Batch Caller
    </h1>
    <p style='color:#94a3b8; font-size:1rem; margin:0;'>
        Upload an Excel file — we'll call your API for every row in Column A.
    </p>
</div>
""", unsafe_allow_html=True)

# ── Layout columns ─────────────────────────────────────────────────────────────
left, right = st.columns([1, 1], gap="large")

# ════════════════════════════════════════════════════════
# LEFT — Configuration
# ════════════════════════════════════════════════════════
with left:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🔗 API Configuration</div>', unsafe_allow_html=True)

    endpoint = st.text_input(
        "API Endpoint",
        placeholder="https://api.example.com/users/{placeholder}/details",
        help="Use `{placeholder}` (or any text in curly braces) where you want each Column A value to be substituted.",
    )

    # Auto-detect placeholder
    import re
    detected = re.findall(r"\{([^}]+)\}", endpoint) if endpoint else []
    if detected:
        st.success(f"✅ Detected placeholder(s): **{', '.join('{'+p+'}' for p in detected)}**")
    elif endpoint:
        st.warning("⚠️ No `{placeholder}` found in the URL. The URL will be called as-is for every row.")

    st.markdown("---")
    st.markdown('<div class="section-title">⚙️ Request Settings</div>', unsafe_allow_html=True)

    col_method, col_delay = st.columns(2)
    with col_method:
        method = st.selectbox("HTTP Method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
    with col_delay:
        delay = st.number_input("Delay between calls (s)", min_value=0.0, max_value=10.0, value=0.3, step=0.1)

    headers_raw = st.text_area(
        "Custom Headers (JSON)",
        placeholder='{"Authorization": "Bearer YOUR_TOKEN", "Content-Type": "application/json"}',
        height=90,
    )

    body_raw = ""
    if method in ("POST", "PUT", "PATCH"):
        body_raw = st.text_area(
            "Request Body (JSON) — use {placeholder} here too",
            placeholder='{"id": "{placeholder}", "action": "lookup"}',
            height=90,
        )

    st.markdown('</div>', unsafe_allow_html=True)

    # ── Excel upload ───────────────────────────────────────────────────────────
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📂 Excel File</div>', unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Upload your Excel file (.xlsx / .xls / .csv)",
        type=["xlsx", "xls", "csv"],
    )

    df_preview = None
    column_values = []

    if uploaded_file:
        try:
            if uploaded_file.name.endswith(".csv"):
                df_preview = pd.read_csv(uploaded_file)
            else:
                df_preview = pd.read_excel(uploaded_file)

            column_values = df_preview.iloc[:, 0].dropna().astype(str).tolist()

            st.success(f"✅ Loaded **{len(column_values)} rows** from Column A")
            with st.expander("Preview Column A values"):
                for i, v in enumerate(column_values[:20], 1):
                    st.markdown(f"`{i}.` {v}")
                if len(column_values) > 20:
                    st.caption(f"… and {len(column_values)-20} more rows")
        except Exception as e:
            st.error(f"Could not read file: {e}")

    st.markdown('</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════
# RIGHT — Results
# ════════════════════════════════════════════════════════
with right:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🚀 Run & Results</div>', unsafe_allow_html=True)

    run_btn = st.button("▶ Start Batch API Calls", use_container_width=True)

    results_placeholder = st.empty()
    stats_placeholder   = st.empty()

    st.markdown('</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════
# Run logic
# ════════════════════════════════════════════════════════
if run_btn:
    # ── Validate ────────────────────────────────────────
    errors = []
    if not endpoint:
        errors.append("Please enter an API endpoint.")
    if not column_values:
        errors.append("Please upload an Excel file with data in Column A.")

    try:
        headers = json.loads(headers_raw) if headers_raw.strip() else {}
    except json.JSONDecodeError:
        errors.append("Custom headers must be valid JSON.")
        headers = {}

    try:
        body_template = json.loads(body_raw) if body_raw.strip() else {}
    except json.JSONDecodeError:
        errors.append("Request body must be valid JSON.")
        body_template = {}

    if errors:
        for e in errors:
            st.error(e)
    else:
        results = []
        success_count = 0
        error_count   = 0

        placeholder_keys = re.findall(r"\{([^}]+)\}", endpoint) or []

        progress_bar = right.progress(0, text="Starting…")
        total = len(column_values)

        def substitute(template_str, value):
            """Replace every {key} in template_str with value."""
            return re.sub(r"\{[^}]+\}", str(value), template_str)

        def substitute_dict(d, value):
            """Recursively substitute placeholders inside a dict/list."""
            if isinstance(d, dict):
                return {k: substitute_dict(v, value) for k, v in d.items()}
            if isinstance(d, list):
                return [substitute_dict(i, value) for i in d]
            if isinstance(d, str):
                return substitute(d, value)
            return d

        result_html_parts = []

        for idx, val in enumerate(column_values):
            url = substitute(endpoint, val)
            body = substitute_dict(body_template, val) if body_template else None

            try:
                resp = requests.request(
                    method,
                    url,
                    headers=headers,
                    json=body if body else None,
                    timeout=15,
                )
                status = resp.status_code
                try:
                    content = json.dumps(resp.json(), indent=2)
                except Exception:
                    content = resp.text[:2000] or "(empty response)"

                row_class = "success" if resp.ok else "error"
                badge_class = "badge-success" if resp.ok else "badge-error"
                badge_label = f"{status} OK" if resp.ok else f"{status} ERR"
                if resp.ok:
                    success_count += 1
                else:
                    error_count += 1

                results.append({
                    "row": idx + 1,
                    "value": val,
                    "url": url,
                    "status": status,
                    "ok": resp.ok,
                    "response": content,
                })

                result_html_parts.append(f"""
                <div class="result-row {row_class}">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="color:#e2e8f0;font-weight:600;">Row {idx+1} — <code style="color:#c4b5fd;">{val}</code></span>
                        <span class="badge {badge_class}">{badge_label}</span>
                    </div>
                    <div style="color:#94a3b8;font-size:0.78rem;margin-top:4px;">🔗 {url}</div>
                    <details style="margin-top:6px;">
                        <summary>View response</summary>
                        <pre style="background:rgba(0,0,0,0.3);padding:10px;border-radius:8px;
                                    font-size:0.78rem;color:#a5f3fc;overflow-x:auto;margin-top:6px;">{content[:1500]}{'…' if len(content)>1500 else ''}</pre>
                    </details>
                </div>
                """)

            except requests.exceptions.RequestException as exc:
                error_count += 1
                results.append({
                    "row": idx + 1,
                    "value": val,
                    "url": url,
                    "status": "ERR",
                    "ok": False,
                    "response": str(exc),
                })
                result_html_parts.append(f"""
                <div class="result-row error">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="color:#e2e8f0;font-weight:600;">Row {idx+1} — <code style="color:#c4b5fd;">{val}</code></span>
                        <span class="badge badge-error">REQUEST ERROR</span>
                    </div>
                    <div style="color:#94a3b8;font-size:0.78rem;margin-top:4px;">🔗 {url}</div>
                    <div style="color:#fca5a5;font-size:0.8rem;margin-top:6px;">{str(exc)}</div>
                </div>
                """)

            # Update progress
            pct = (idx + 1) / total
            progress_bar.progress(pct, text=f"Processing row {idx+1} of {total}…")

            # Live update results
            results_placeholder.markdown(
                "".join(result_html_parts), unsafe_allow_html=True
            )

            if delay > 0 and idx < total - 1:
                time.sleep(delay)

        progress_bar.empty()

        # ── Summary stats ────────────────────────────────
        stats_placeholder.markdown(f"""
        <div style='display:flex;gap:1rem;margin-top:0.5rem;'>
            <div style='flex:1;background:rgba(16,185,129,0.15);border:1px solid #065f46;
                        border-radius:12px;padding:1rem;text-align:center;'>
                <div style='font-size:2rem;font-weight:700;color:#10b981;'>{success_count}</div>
                <div style='color:#6ee7b7;font-size:0.8rem;'>Successful</div>
            </div>
            <div style='flex:1;background:rgba(239,68,68,0.15);border:1px solid #7f1d1d;
                        border-radius:12px;padding:1rem;text-align:center;'>
                <div style='font-size:2rem;font-weight:700;color:#ef4444;'>{error_count}</div>
                <div style='color:#fca5a5;font-size:0.8rem;'>Errors</div>
            </div>
            <div style='flex:1;background:rgba(99,102,241,0.15);border:1px solid #3730a3;
                        border-radius:12px;padding:1rem;text-align:center;'>
                <div style='font-size:2rem;font-weight:700;color:#a78bfa;'>{total}</div>
                <div style='color:#c4b5fd;font-size:0.8rem;'>Total Rows</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Download results as CSV ──────────────────────
        df_results = pd.DataFrame(results)
        csv_bytes = df_results.to_csv(index=False).encode("utf-8")

        right.download_button(
            label="⬇ Download Results as CSV",
            data=csv_bytes,
            file_name="api_batch_results.csv",
            mime="text/csv",
            use_container_width=True,
        )
