# Des Moines Data Monitor

A real-time Streamlit dashboard for monitoring CO2 data uploads to AWS S3.

## Features

- 📊 Real-time checkpoint tracking
- 📤 Monitor S3 uploaded files
- 💾 Track local batch files
- ✅ Sync status visualization
- ⏳ Pending uploads alert
- 🔄 Live data refresh

## Setup

### Prerequisites

- Python 3.8+
- AWS account with S3 access and Secrets Manager permissions

### Step 1: Store Credentials Securely

Instead of using a JSON file, store your credentials in AWS Secrets Manager:

```bash
python setup_secrets.py
```

This will prompt you for your credentials and store them securely.

### Step 2: Local Development

1. Clone the repo:
```bash
git clone https://github.com/pavan-249/des-moines-data-monitor.git
cd des-moines-data-monitor
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run the dashboard:
```bash
streamlit run dashboard.py
```

Open your browser at `http://localhost:8501`

The dashboard will automatically read credentials from AWS Secrets Manager.

## Deployment

### Streamlit Cloud

1. Push to GitHub (no credentials needed!)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select this repo and deploy
4. In the **Settings → Secrets**, add:

```
AWS_ACCESS_KEY_ID = "your-aws-user-access-key"
AWS_SECRET_ACCESS_KEY = "your-aws-user-secret-key"
AWS_REGION = "us-west-2"
S3_BUCKET_NAME = "des-moines-test"
S3_PREFIX = "licor/raw"
AWS_SECRET_NAME = "des-moines/s3-credentials"
```

The dashboard will use these to authenticate and retrieve S3 credentials from Secrets Manager.

## Files

- `dashboard.py` - Main Streamlit dashboard
- `requirements.txt` - Python dependencies
- `.gitignore` - Git ignore rules
- `README.md` - This file

## Security

Credentials are stored securely in AWS Secrets Manager, not in the codebase:
- ✅ No `aws_creds.json` needed
- ✅ Safe to commit to GitHub
- ✅ Credentials never exposed in logs or version history
- ✅ Streamlit Cloud accesses Secrets Manager safely

Never commit actual credentials to version control!
