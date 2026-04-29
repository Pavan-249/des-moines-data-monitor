import streamlit as st
import json
import os
from datetime import datetime
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# -------------------------
# Config (same as schedule_upload_co2.py)
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
st.set_page_config(page_title="CO2 Data Upload Dashboard", layout="wide")
st.title("📊 CO2 Data Upload Status")

try:
    s3 = create_s3_client()
    
    # Row 1: Checkpoint Status
    st.subheader("Checkpoint Status")
    offset, updated_at = load_checkpoint()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Byte Offset", f"{offset:,}")
    with col2:
        st.metric("Last Updated", updated_at)
    
    # Row 2: File size info
    if os.path.exists(input_file_name):
        file_size = os.path.getsize(input_file_name)
        progress = (offset / file_size * 100) if file_size > 0 else 0
        st.progress(progress / 100, text=f"Input file progress: {progress:.1f}%")
    
    # Row 3: S3 vs Local
    st.divider()
    col1, col2 = st.columns(2)
    
    s3_files = list_s3_uploaded_batches(s3)
    local_files = get_local_batches()
    
    with col1:
        st.subheader(f"📤 S3 Uploaded ({len(s3_files)} files)")
        if s3_files:
            s3_df = pd.DataFrame(s3_files)
            s3_df["size"] = s3_df["size"].apply(lambda x: f"{x:,.0f} bytes")
            s3_df["last_modified"] = pd.to_datetime(s3_df["last_modified"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(s3_df, use_container_width=True, hide_index=True)
        else:
            st.info("No files uploaded yet")
    
    with col2:
        st.subheader(f"💾 Local Batches ({len(local_files)} files)")
        if local_files:
            local_df = pd.DataFrame(local_files)
            local_df["size"] = local_df["size"].apply(lambda x: f"{x:,.0f} bytes")
            local_df["last_modified"] = local_df["last_modified"].dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(local_df, use_container_width=True, hide_index=True)
        else:
            st.info("No local batches found")
    
    # Row 4: Sync Status
    st.divider()
    st.subheader("Sync Status")
    
    s3_file_names = {f["file"] for f in s3_files}
    local_file_names = {f["file"] for f in local_files}
    
    synced = s3_file_names & local_file_names
    missing_in_s3 = local_file_names - s3_file_names
    orphan_in_s3 = s3_file_names - local_file_names
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("✅ Synced", len(synced))
    with col2:
        st.metric("⏳ Pending Upload", len(missing_in_s3))
    with col3:
        st.metric("🔍 S3 Only", len(orphan_in_s3))
    
    if missing_in_s3:
        st.warning(f"**Pending upload:** {', '.join(sorted(missing_in_s3))}")
    
    if orphan_in_s3:
        st.info(f"**In S3 only:** {', '.join(sorted(orphan_in_s3))}")
    
    # Auto-refresh
    st.divider()
    if st.button("🔄 Refresh Now"):
        st.rerun()
    
    st.caption("Auto-refresh every 60 seconds")
    st.session_state.refresh_time = st.session_state.get("refresh_time", datetime.now())

except Exception as e:
    st.error(f"Error connecting to S3: {str(e)}")
    st.info("Make sure `aws_creds.json` is configured correctly and has the right permissions.")
