import requests
import pandas as pd
import os
import boto3
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO  # For handling CSV data in-memory

# Load environment variables
load_dotenv()


# AWS Credentials
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID") 
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("AWS_S3_BUCKET_NAME")

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# FRED API Key
FRED_KEY = os.getenv("FRED_API_KEY")

# Define S3 folder paths
S3_PATHS = {
    "Daily": "DailyCurrencyExchange/",
    "Monthly": "MonthlyCurrencyExchange/"
}

def fetch_series_data(series_id):
    """
    Fetches time series data from the FRED API.
    Returns: A pandas DataFrame containing the series data, or None if an error occurs.
    """
    start_date = "2020-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")  # Get today's date

    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&observation_start={start_date}&observation_end={end_date}"
           f"&file_type=json&api_key={FRED_KEY}")

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP issues
        data = response.json().get("observations", [])
        if not data:
            print(f"⚠️ No data available for {series_id}. Skipping...")
            return None
        df = pd.DataFrame(data)[["date", "value"]]
        return df

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for {series_id}. Error: {e}")
        return None

def upload_to_s3(df, series_id):
    """
    Uploads the DataFrame as a CSV to the appropriate S3 folder.
    """
    if df is None or df.empty:
        return  # Skip uploading if no data

    # Determine S3 folder based on series type
    s3_folder = S3_PATHS["Daily"] if series_id.startswith("D") else S3_PATHS["Monthly"]
    s3_key = f"{s3_folder}{series_id}.csv"  # Full S3 object key

    # Convert DataFrame to CSV (in-memory)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload to S3
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"Successfully uploaded {series_id}.csv to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {series_id}.csv to S3. Error: {e}")

# List of FRED series to fetch
series_list = ["DEXUSEU", "DEXUSUK", "DEXINUS", "EXINUS", "EXUSEU", "EXUSUK"]

# Fetch and upload data for all series
for series_id in series_list:
    print(f"Fetching data for {series_id}...")
    df = fetch_series_data(series_id)
    upload_to_s3(df, series_id)

