import streamlit as st
import json
import os
from datetime import datetime
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import base64
from io import BytesIO

# -------------------------
# Page Config & Styling
# -------------------------
st.set_page_config(
    page_title="Des Moines Data Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for beautiful styling
def get_custom_css():
    return """
    <style>
    * {
        margin: 0;
        padding: 0;
    }
    
    /* Full page background */
    [data-testid="stAppViewContainer"]::before {
        content: "";
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(135deg, #0a1628 0%, #1a2a4a 50%, #0d1f3a 100%),
                    url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 600"><defs><linearGradient id="sky" x1="0%" y1="0%" x2="0%" y2="100%"><stop offset="0%" style="stop-color:%23667eea;stop-opacity:0.1"/><stop offset="100%" style="stop-color:%23764ba2;stop-opacity:0.15"/></linearGradient></defs><rect width="1200" height="600" fill="url(%23sky)"/><circle cx="100" cy="100" r="80" fill="%23667eea" opacity="0.1"/><circle cx="1100" cy="150" r="120" fill="%23764ba2" opacity="0.08"/></svg>');
        background-attachment: fixed;
        background-size: cover;
        background-position: center;
        z-index: -1;
    }
    
    [data-testid="stAppViewContainer"] {
        background: transparent !important;
    }
    
    [data-testid="stMainBlockContainer"] {
        padding: 2rem 1rem;
    }
    
    /* Header Title Card */
    .header-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 3rem 2rem;
        border-radius: 20px;
        margin-bottom: 2.5rem;
        box-shadow: 0 20px 60px rgba(102, 126, 234, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.2);
        animation: slideDown 0.7s cubic-bezier(0.34, 1.56, 0.64, 1);
        text-align: center;
        backdrop-filter: blur(10px);
    }
    
    .header-container h1 {
        font-size: 3em;
        font-weight: 900;
        color: white;
        margin-bottom: 0.5rem;
        text-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
        letter-spacing: 2px;
    }
    
    .header-container p {
        font-size: 1.3em;
        color: rgba(255, 255, 255, 0.95);
        font-weight: 300;
        letter-spacing: 1px;
    }
    
    /* Section Headers */
    .section-header {
        color: #a78bfa;
        font-size: 1.4em;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 3px;
        margin: 2.5rem 0 1.5rem 0;
        text-shadow: 0 2px 10px rgba(167, 139, 250, 0.3);
        padding-left: 1rem;
        border-left: 5px solid #667eea;
    }
    
    /* Metric Cards */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.2) 0%, rgba(118, 75, 162, 0.2) 100%);
        border: 2px solid rgba(102, 126, 234, 0.5);
        padding: 1.8rem;
        border-radius: 16px;
        backdrop-filter: blur(20px);
        transition: all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
        box-shadow: 0 8px 32px rgba(102, 126, 234, 0.25);
        animation: fadeInUp 0.6s ease-out forwards;
    }
    
    [data-testid="metric-container"]:hover {
        transform: translateY(-8px) scale(1.02);
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.3) 0%, rgba(118, 75, 162, 0.3) 100%);
        border-color: rgba(102, 126, 234, 0.9);
        box-shadow: 0 20px 60px rgba(102, 126, 234, 0.5);
    }
    
    /* Info Box */
    .info-box {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.15) 0%, rgba(118, 75, 162, 0.15) 100%);
        border: 2px solid rgba(102, 126, 234, 0.4);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
    }
    
    /* Data Tables */
    [data-testid="dataframe"] {
        background: rgba(30, 50, 80, 0.7) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(102, 126, 234, 0.3) !important;
        overflow: hidden !important;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3) !important;
    }
    
    [data-testid="dataframe"] thead {
        background: linear-gradient(90deg, rgba(102, 126, 234, 0.3), rgba(118, 75, 162, 0.3)) !important;
        border-bottom: 2px solid rgba(102, 126, 234, 0.5) !important;
    }
    
    [data-testid="dataframe"] tbody tr:hover {
        background: rgba(102, 126, 234, 0.2) !important;
    }
    
    /* Progress Bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        box-shadow: 0 0 20px rgba(102, 126, 234, 0.5);
    }
    
    /* Info Boxes */
    [data-testid="stAlert"] {
        background: rgba(30, 50, 80, 0.8) !important;
        border-left: 5px solid #667eea !important;
        border-radius: 10px !important;
        padding: 1.2rem !important;
        margin: 1rem 0 !important;
        backdrop-filter: blur(10px);
    }
    
    /* Buttons */
    button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 700 !important;
        transition: all 0.3s ease !important;
    }
    
    button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 24px rgba(102, 126, 234, 0.4) !important;
    }
    
    /* Dividers */
    hr {
        margin: 2rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.5), transparent);
    }
    
    /* Animations */
    @keyframes slideDown {
        from {
            opacity: 0;
            transform: translateY(-40px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(40px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    /* Status Cards */
    .sync-card {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.15) 0%, rgba(118, 75, 162, 0.15) 100%);
        border: 2px solid rgba(102, 126, 234, 0.3);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0;
        backdrop-filter: blur(10px);
    }
    
    .sync-card.good {
        border-color: rgba(16, 185, 129, 0.5);
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.1) 0%, rgba(52, 211, 153, 0.1) 100%);
    }
    
    .sync-card.warning {
        border-color: rgba(217, 119, 6, 0.5);
        background: linear-gradient(135deg, rgba(217, 119, 6, 0.1) 0%, rgba(251, 191, 36, 0.1) 100%);
    }
    
    .sync-card.info {
        border-color: rgba(59, 130, 246, 0.5);
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(96, 165, 250, 0.1) 100%);
    }
    
    .sync-title {
        font-weight: 700;
        font-size: 1.1em;
        margin-bottom: 0.5rem;
    }
    
    .sync-description {
        font-size: 0.9em;
        opacity: 0.85;
        line-height: 1.5;
    }
    </style>
    """

# Inject custom CSS
st.markdown(get_custom_css(), unsafe_allow_html=True)

# -------------------------
# Config
# -------------------------
checkpoint_file_name = "checkpoint.json"
batch_dir = "batches"
bucket_name = os.getenv("S3_BUCKET_NAME", "des-moines-test")
s3_prefix = os.getenv("S3_PREFIX", "licor/raw")
input_file_name = "2026Feb12-25_CO2-46_Duwamish.txt"

# -------------------------
# Load AWS credentials
# -------------------------
@st.cache_resource
def get_secrets():
    """Get AWS credentials from Secrets Manager or environment variables."""
    secret_name = os.getenv("AWS_SECRET_NAME", "des-moines/s3-credentials")
    
    # Try to read from Secrets Manager first
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name='us-west-2')
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return {
            "aws_access_key_id": secret.get("aws_access_key_id"),
            "aws_secret_access_key": secret.get("aws_secret_access_key"),
            "region": secret.get("region", "us-west-2")
        }
    except ClientError:
        # Fall back to environment variables
        return {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_REGION", "us-west-2")
        }

def create_s3_client():
    secrets = get_secrets()
    return boto3.client(
        "s3",
        aws_access_key_id=secrets["aws_access_key_id"],
        aws_secret_access_key=secrets["aws_secret_access_key"],
        region_name=secrets["region"]
    )

# -------------------------
# Load checkpoint
# -------------------------
def load_checkpoint():
    if os.path.exists(checkpoint_file_name):
        with open(checkpoint_file_name, "r") as f:
            data = json.load(f)
            return data.get("offset", 0), data.get("updated_at", "N/A")
    return 0, "N/A"

# -------------------------
# S3 functions
# -------------------------
def list_s3_uploaded_batches(s3):
    uploaded_files = []
    paginator = s3.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            uploaded_files.append({
                "file": os.path.basename(obj["Key"]),
                "size": obj["Size"],
                "last_modified": obj["LastModified"]
            })
    
    return uploaded_files

def get_local_batches():
    local_files = []
    if os.path.exists(batch_dir):
        for file_name in sorted(os.listdir(batch_dir)):
            if file_name.endswith(".txt"):
                file_path = os.path.join(batch_dir, file_name)
                size = os.path.getsize(file_path)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                local_files.append({
                    "file": file_name,
                    "size": size,
                    "last_modified": mod_time
                })
    return local_files

# -------------------------
# Streamlit Dashboard
# -------------------------
st.markdown(get_custom_css(), unsafe_allow_html=True)

# Header
st.markdown("""
    <div class="header-container">
        <h1>📊 Des Moines Data Monitor</h1>
        <p>Real-time CO2 Data Upload Status</p>
    </div>
""", unsafe_allow_html=True)

try:
    s3 = create_s3_client()
    
    # ===== CHECKPOINT STATUS =====
    st.markdown('<p class="section-header">🎯 Checkpoint Status</p>', unsafe_allow_html=True)
    
    offset, updated_at = load_checkpoint()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📍 Byte Offset", f"{offset:,}")
    with col2:
        st.metric("🕐 Last Updated", updated_at.split("T")[0] if "T" in updated_at else updated_at)
    with col3:
        if os.path.exists(input_file_name):
            file_size = os.path.getsize(input_file_name)
            progress_pct = (offset / file_size * 100) if file_size > 0 else 0
            st.metric("📈 Progress", f"{progress_pct:.1f}%")
    
    # Progress visualization
    if os.path.exists(input_file_name):
        file_size = os.path.getsize(input_file_name)
        progress = (offset / file_size) if file_size > 0 else 0
        st.progress(min(progress, 1.0), text=f"File Processing: {min(progress * 100, 100):.1f}%")
    
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # ===== UPLOAD STATUS =====
    st.markdown('<p class="section-header">📤 Upload Status</p>', unsafe_allow_html=True)
    
    s3_files = list_s3_uploaded_batches(s3)
    local_files = get_local_batches()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"<h3 style='color: #a78bfa; margin-top: 0;'>☁️ S3 Uploaded ({len(s3_files)} files)</h3>", unsafe_allow_html=True)
        if s3_files:
            s3_df = pd.DataFrame(s3_files)
            s3_df["size"] = s3_df["size"].apply(lambda x: f"{x:,.0f} bytes")
            s3_df["last_modified"] = pd.to_datetime(s3_df["last_modified"]).dt.strftime("%Y-%m-%d %H:%M")
            s3_df = s3_df[["file", "size", "last_modified"]]
            st.dataframe(s3_df, use_container_width=True, hide_index=True)
        else:
            st.info("✨ No files uploaded to S3 yet")
    
    with col2:
        st.markdown(f"<h3 style='color: #a78bfa; margin-top: 0;'>💾 Local Batches ({len(local_files)} files)</h3>", unsafe_allow_html=True)
        if local_files:
            local_df = pd.DataFrame(local_files)
            local_df["size"] = local_df["size"].apply(lambda x: f"{x:,.0f} bytes")
            local_df["last_modified"] = local_df["last_modified"].dt.strftime("%Y-%m-%d %H:%M")
            local_df = local_df[["file", "size", "last_modified"]]
            st.dataframe(local_df, use_container_width=True, hide_index=True)
        else:
            st.info("✨ No local batches waiting")
    
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # ===== SYNC STATUS =====
    st.markdown('<p class="section-header">✅ Sync Overview</p>', unsafe_allow_html=True)
    
    s3_file_names = {f["file"] for f in s3_files}
    local_file_names = {f["file"] for f in local_files}
    
    synced = s3_file_names & local_file_names
    missing_in_s3 = local_file_names - s3_file_names
    orphan_in_s3 = s3_file_names - local_file_names
    
    # Clear explanation of sync status
    st.markdown("""
    <div style='background: rgba(102, 126, 234, 0.1); border-left: 5px solid #667eea; padding: 1rem; border-radius: 8px; margin-bottom: 1.5rem;'>
        <p style='margin: 0; color: #a78bfa; font-size: 0.95em;'>
        <strong>What this means:</strong><br>
        • <strong>✅ Synced:</strong> Files that exist both locally AND in S3 (fully uploaded)<br>
        • <strong>⏳ Pending:</strong> Files waiting locally but NOT yet in S3 (need upload)<br>
        • <strong>🔍 Orphans:</strong> Files in S3 but NOT on local disk (uploaded previously, not stored locally)
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Status cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        status_class = "good" if len(missing_in_s3) == 0 else "warning"
        st.markdown(f"""
        <div class="sync-card {status_class}">
            <div class="sync-title">✅ Synced Files</div>
            <div style='font-size: 2.5em; font-weight: 900; color: #10b981; margin: 0.5rem 0;'>{len(synced)}</div>
            <div class="sync-description">Files successfully uploaded to S3</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        status_class = "warning" if len(missing_in_s3) > 0 else "good"
        st.markdown(f"""
        <div class="sync-card {status_class}">
            <div class="sync-title">⏳ Pending Uploads</div>
            <div style='font-size: 2.5em; font-weight: 900; color: #f59e0b; margin: 0.5rem 0;'>{len(missing_in_s3)}</div>
            <div class="sync-description">Files waiting to be uploaded</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="sync-card info">
            <div class="sync-title">🔍 Orphaned Files</div>
            <div style='font-size: 2.5em; font-weight: 900; color: #3b82f6; margin: 0.5rem 0;'>{len(orphan_in_s3)}</div>
            <div class="sync-description">Files only in S3 (archive)</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Detailed alerts
    col1, col2 = st.columns(2)
    
    with col1:
        if missing_in_s3:
            pending_list = "\n• ".join(sorted(list(missing_in_s3)[:5]))
            if len(missing_in_s3) > 5:
                pending_list += f"\n• ... and {len(missing_in_s3)-5} more"
            st.warning(f"**📤 These files need upload:**\n• {pending_list}")
        else:
            st.success("**✨ All local files are uploaded!**")
    
    with col2:
        if orphan_in_s3:
            orphan_list = "\n• ".join(sorted(list(orphan_in_s3)[:5]))
            if len(orphan_in_s3) > 5:
                orphan_list += f"\n• ... and {len(orphan_in_s3)-5} more"
            st.info(f"**📦 Archive (in S3 only):**\n• {orphan_list}")
        else:
            st.info("**✨ No orphaned files in S3**")
    
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # ===== QUICK ACTIONS =====
    st.markdown('<p class="section-header">🔄 Actions</p>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button("🔄 Refresh Dashboard", use_container_width=True):
            st.rerun()
    
    with col2:
        st.caption("⏱️ Last refresh:")
    
    with col3:
        st.caption(datetime.now().strftime("%H:%M:%S"))

except Exception as e:
    st.markdown("""
        <div style='background: rgba(239, 68, 68, 0.1); border-left: 5px solid #ef4444; padding: 1.5rem; border-radius: 8px;'>
            <h3 style='color: #fca5a5; margin: 0 0 0.5rem 0;'>❌ Error Connecting to S3</h3>
            <p style='color: #fecaca; margin: 0;'>Make sure AWS credentials are configured correctly with Secrets Manager or environment variables.</p>
        </div>
    """, unsafe_allow_html=True)
    st.error(f"Details: {str(e)}")
