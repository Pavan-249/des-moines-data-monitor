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
- AWS account with S3 access

### Local Development

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

4. Set environment variables:
```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_REGION="us-west-2"
export S3_BUCKET_NAME="des-moines-test"
export S3_PREFIX="licor/raw"
```

5. Run the dashboard:
```bash
streamlit run dashboard.py
```

Open your browser at `http://localhost:8501`

## Deployment

### Streamlit Cloud

1. Push to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select this repo
4. Add secrets in the Settings tab for AWS credentials

## Files

- `dashboard.py` - Main Streamlit dashboard
- `requirements.txt` - Python dependencies
- `.gitignore` - Git ignore rules
- `README.md` - This file

## Security

Never commit `aws_creds.json` or AWS credentials to version control. Always use environment variables or Streamlit secrets.
