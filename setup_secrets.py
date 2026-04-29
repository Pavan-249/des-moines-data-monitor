#!/usr/bin/env python3
"""
Script to store AWS S3 credentials securely in AWS Secrets Manager.
Run this once to set up your credentials.
"""

import json
import boto3
from botocore.exceptions import ClientError

def store_credentials_in_secrets_manager():
    """Store S3 credentials in AWS Secrets Manager."""
    
    print("=" * 60)
    print("AWS Secrets Manager Setup")
    print("=" * 60)
    print()
    
    # Get credentials from user
    print("Enter your AWS S3 credentials:")
    access_key = input("AWS Access Key ID: ").strip()
    secret_key = input("AWS Secret Access Key: ").strip()
    region = input("AWS Region (default: us-west-2): ").strip() or "us-west-2"
    
    credentials = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region": region
    }
    
    # Create a temporary S3 client with these credentials to validate
    print("\nValidating credentials...")
    try:
        s3_test = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        s3_test.list_buckets()
        print("✓ Credentials validated!")
    except Exception as e:
        print(f"✗ Error validating credentials: {e}")
        return False
    
    # Store in Secrets Manager
    secret_name = "des-moines/s3-credentials"
    
    print(f"\nStoring credentials in Secrets Manager as: {secret_name}")
    
    try:
        client = boto3.client('secretsmanager', region_name=region)
        
        try:
            # Try to update existing secret
            client.update_secret(
                SecretId=secret_name,
                SecretString=json.dumps(credentials)
            )
            print(f"✓ Secret updated: {secret_name}")
        except client.exceptions.ResourceNotFoundException:
            # Create new secret
            client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(credentials),
                Description="S3 credentials for Des Moines data monitor"
            )
            print(f"✓ Secret created: {secret_name}")
        
        print("\n" + "=" * 60)
        print("Setup complete!")
        print("=" * 60)
        print("\nFor Streamlit Cloud deployment:")
        print("  1. Go to https://share.streamlit.io")
        print("  2. Add this secret to your app settings:")
        print(f"     AWS_ACCESS_KEY_ID = <your main AWS user access key>")
        print(f"     AWS_SECRET_ACCESS_KEY = <your main AWS user secret key>")
        print(f"     AWS_REGION = {region}")
        print("\nThe dashboard will now read S3 credentials from Secrets Manager.")
        print()
        return True
        
    except Exception as e:
        print(f"✗ Error storing in Secrets Manager: {e}")
        return False

if __name__ == "__main__":
    store_credentials_in_secrets_manager()
