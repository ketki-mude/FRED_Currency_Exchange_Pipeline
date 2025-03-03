import requests
import pandas as pd
import os
from datetime import datetime

# Set your FRED API key
FRED_KEY = "91d698d905ae8dcde262e03c9bb1b878"

# Directory structure
SAVE_PATHS = {
    "Daily": os.path.join(os.getcwd(), "saved_series/Daily"),
    "Monthly": os.path.join(os.getcwd(), "saved_series/Monthly")
}

# Ensure directories exist
for path in SAVE_PATHS.values():
    os.makedirs(path, exist_ok=True)

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

def save_series_to_csv(df, series_id):
    """
    Saves a DataFrame to a CSV file.
    """
    if df is None or df.empty:
        return  # Skip saving if no data
    save_dir = SAVE_PATHS["Daily"] if series_id.startswith("D") else SAVE_PATHS["Monthly"]
    filepath = os.path.join(save_dir, f"{series_id}.csv")
    df.to_csv(filepath, index=False)
    print(f"Data saved: {filepath}")

# List of FRED series to fetch
series_list = ["DEXUSEU", "DEXUSUK", "DEXINUS", "EXINUS", "EXUSEU", "EXUSUK"]

# Fetch and save data for all series
for series_id in series_list:
    print(f"Fetching data for {series_id}...")
    df = fetch_series_data(series_id)
    save_series_to_csv(df, series_id)
    