from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# S3 paths and corresponding Snowflake stages
STAGES = {
    "DailyCurrency_RAW_Stage": "s3://fredcurrencyexhange/DailyCurrencyExchange/",
    "MonthlyCurrencyExchange_RAW_Stage": "s3://fredcurrencyexhange/MonthlyCurrencyExchange/"
}

# Connect to Snowflake
try:
    session = Session.builder.configs(snowflake_params).create()
    print("✅ Snowflake connection successful!")
except Exception as e:
    print("❌ Snowflake connection failed:", e)
    exit()

# Create external stages
for stage, s3_path in STAGES.items():
    create_stage_sql = f"""
    CREATE OR REPLACE STAGE {stage}
    STORAGE_INTEGRATION = fred_s3_integration
    URL = '{s3_path}'
    FILE_FORMAT = CSV_FORMAT;
    """
    session.sql(create_stage_sql).collect()
    print(f"✅ Created/Verified stage: {stage}")

# Close session
session.close()
print("🔄 Snowflake session closed.")
