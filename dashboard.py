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

# Custom CSS for beautiful styling with background
def get_custom_css():
    return """
    <style>
    /* Background image with overlay */
    body {
        background-image: url('https://images.unsplash.com/photo-1556388205-94acc2d77214?w=1200&q=80');
        background-attachment: fixed;
        background-size: cover;
        background-position: center;
    }
    
    .main {
        background: linear-gradient(135deg, rgba(10,25,50,0.92) 0%, rgba(20,40,70,0.95) 100%);
    }
    
    /* Header styling */
    .header-title {
        text-align: center;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        color: white;
        box-shadow: 0 8px 32px rgba(102, 126, 234, 0.3);
        animation: slideDown 0.6s ease-out;
    }
    
    .header-title h1 {
        margin: 0;
        font-size: 2.5em;
        font-weight: 800;
        letter-spacing: 1px;
    }
    
    .header-subtitle {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1em;
        margin-top: 0.5rem;
    }
    
    /* Metric cards */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.15) 0%, rgba(118, 75, 162, 0.15) 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid rgba(102, 126, 234, 0.3);
        backdrop-filter: blur(10px);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        animation: fadeInUp 0.6s ease-out forwards;
    }
    
    [data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.25) 0%, rgba(118, 75, 162, 0.25) 100%);
        border-color: rgba(102, 126, 234, 0.6);
        box-shadow: 0 12px 24px rgba(102, 126, 234, 0.2);
    }
    
    /* Dataframe styling */
    [data-testid="dataframe"] {
        background: rgba(30, 50, 80, 0.6) !important;
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid rgba(102, 126, 234, 0.3);
        animation: fadeInUp 0.8s ease-out;
    }
    
    /* Divider */
    hr {
        margin: 2rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.5), transparent);
    }
    
    /* Status badges */
    .status-badge {
        display: inline-block;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9em;
    }
    
    .status-synced {
        background: linear-gradient(135deg, rgba(52, 211, 153, 0.3), rgba(16, 185, 129, 0.3));
        color: #6ee7b7;
        border: 1px solid rgba(16, 185, 129, 0.5);
    }
    
    .status-pending {
        background: linear-gradient(135deg, rgba(251, 191, 36, 0.3), rgba(217, 119, 6, 0.3));
        color: #fcd34d;
        border: 1px solid rgba(217, 119, 6, 0.5);
    }
    
    .status-orphan {
        background: linear-gradient(135deg, rgba(96, 165, 250, 0.3), rgba(59, 130, 246, 0.3));
        color: #93c5fd;
        border: 1px solid rgba(59, 130, 246, 0.5);
    }
    
    /* Section headers */
    .section-header {
        color: #a78bfa;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin: 1.5rem 0 1rem 0;
        font-size: 1.2em;
        text-shadow: 0 2px 8px rgba(167, 139, 250, 0.3);
    }
    
    /* Animations */
    @keyframes slideDown {
        from {
            opacity: 0;
            transform: translateY(-30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes pulse {
        0%, 100% {
            opacity: 1;
        }
        50% {
            opacity: 0.7;
        }
    }
    
    /* Progress bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Info/Warning boxes */
    [data-testid="stAlert"] {
        background: rgba(30, 50, 80, 0.7) !important;
        border-left: 4px solid #667eea !important;
        border-radius: 8px !important;
    }
    </style>
    
    <script>
    // Smooth scroll behavior
    document.documentElement.style.scrollBehavior = 'smooth';
    </script>
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
st.markdown('<div class="header-title"><h1>📊 Des Moines Data Monitor</h1><p class="header-subtitle">Real-time CO2 Data Upload Status</p></div>', unsafe_allow_html=True)

try:
    s3 = create_s3_client()
    
    # Row 1: Checkpoint Status
    st.markdown('<p class="section-header">🎯 Checkpoint Status</p>', unsafe_allow_html=True)
    offset, updated_at = load_checkpoint()
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.metric("📍 Byte Offset", f"{offset:,}")
    with col2:
        st.metric("🕐 Last Updated", updated_at.split("T")[0] if "T" in updated_at else updated_at)
    with col3:
        if os.path.exists(input_file_name):
            file_size = os.path.getsize(input_file_name)
            progress_pct = (offset / file_size * 100) if file_size > 0 else 0
            st.metric("📈 Progress", f"{progress_pct:.1f}%")
    
    # Progress bar
    if os.path.exists(input_file_name):
        file_size = os.path.getsize(input_file_name)
        progress = (offset / file_size) if file_size > 0 else 0
        st.progress(min(progress, 1.0), text=f"Input file progress: {min(progress * 100, 100):.1f}%")
    
    # Row 2: S3 vs Local
    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown('<p class="section-header">📤 Upload Status</p>', unsafe_allow_html=True)
    
    s3_files = list_s3_uploaded_batches(s3)
    local_files = get_local_batches()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"<p style='color: #a78bfa; font-weight: 700; font-size: 1.1em;'>☁️ S3 Uploaded ({len(s3_files)} files)</p>", unsafe_allow_html=True)
        if s3_files:
            s3_df = pd.DataFrame(s3_files)
            s3_df["size"] = s3_df["size"].apply(lambda x: f"{x:,.0f} bytes")
            s3_df["last_modified"] = pd.to_datetime(s3_df["last_modified"]).dt.strftime("%Y-%m-%d %H:%M")
            s3_df = s3_df[["file", "size", "last_modified"]]
            st.dataframe(s3_df, use_container_width=True, hide_index=True)
        else:
            st.info("✨ No files uploaded yet")
    
    with col2:
        st.markdown(f"<p style='color: #a78bfa; font-weight: 700; font-size: 1.1em;'>💾 Local Batches ({len(local_files)} files)</p>", unsafe_allow_html=True)
        if local_files:
            local_df = pd.DataFrame(local_files)
            local_df["size"] = local_df["size"].apply(lambda x: f"{x:,.0f} bytes")
            local_df["last_modified"] = local_df["last_modified"].dt.strftime("%Y-%m-%d %H:%M")
            local_df = local_df[["file", "size", "last_modified"]]
            st.dataframe(local_df, use_container_width=True, hide_index=True)
        else:
            st.info("✨ No local batches found")
    
    # Row 3: Sync Status
    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown('<p class="section-header">✅ Sync Status</p>', unsafe_allow_html=True)
    
    s3_file_names = {f["file"] for f in s3_files}
    local_file_names = {f["file"] for f in local_files}
    
    synced = s3_file_names & local_file_names
    missing_in_s3 = local_file_names - s3_file_names
    orphan_in_s3 = s3_file_names - local_file_names
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("✅ Synced", len(synced), delta="In sync" if len(missing_in_s3) == 0 else f"{len(missing_in_s3)} pending")
    with col2:
        st.metric("⏳ Pending Upload", len(missing_in_s3))
    with col3:
        st.metric("🔍 S3 Only", len(orphan_in_s3))
    
    # Alerts for pending
    if missing_in_s3:
        with st.container():
            st.warning(f"**⚠️ Pending upload:** {', '.join(sorted(list(missing_in_s3)[:3]))}" + 
                      (f"... +{len(missing_in_s3)-3} more" if len(missing_in_s3) > 3 else ""))
    
    if orphan_in_s3:
        with st.container():
            st.info(f"**ℹ️ In S3 only:** {', '.join(sorted(list(orphan_in_s3)[:3]))}" + 
                   (f"... +{len(orphan_in_s3)-3} more" if len(orphan_in_s3) > 3 else ""))
    
    # Row 4: Quick Actions
    st.markdown('<hr>', unsafe_allow_html=True)
    st.markdown('<p class="section-header">🔄 Actions</p>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🔄 Refresh Dashboard", use_container_width=True):
            st.rerun()
    with col2:
        st.info(f"⏱️ Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
    with col3:
        st.caption("💡 Dashboard auto-updates every minute")

except Exception as e:
    st.error(f"❌ Error connecting to S3: {str(e)}")
    st.info("⚠️ Make sure AWS credentials are configured correctly with Secrets Manager or environment variables.")
