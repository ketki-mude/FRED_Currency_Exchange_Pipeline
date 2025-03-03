import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": "FRED_DB",
    "schema": "EXTERNAL",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

def create_tasks(session):
    # Set the appropriate role and warehouse
    session.sql("USE ROLE FRED_ROLE").collect()
    session.sql("USE WAREHOUSE FRED_WH").collect()
    session.sql("USE DATABASE FRED_DB").collect()
    
    # Task 1: Load data from S3 to Raw Tables
    session.sql("""
    CREATE OR REPLACE TASK LOAD_RAW_DATA_TASK
        WAREHOUSE = FRED_WH
        SCHEDULE = 'USING CRON 0 6 * * *' -- Run daily at 6 AM
    AS
        CALL SYSTEM$EXECUTE_PYTHON_FUNCTION(
            'steps.04_LoadRAWTables',
            'main',
            ARRAY_CONSTRUCT()
        );
    """).collect()
    
    
    # Task 3: Create Views with Null/Missing Value Handling
    # session.sql("""
    # CREATE OR REPLACE TASK CREATE_VIEWS_TASK
    #     WAREHOUSE = FRED_WH
    #     AFTER APPLY_CURRENCY_CONVERSION_TASK
    # AS
    #     EXECUTE IMMEDIATE $$
        
    # """).collect()
    
    # Task 4: Load Data from Views to Harmonized Tables
    session.sql("""
    CREATE OR REPLACE TASK LOAD_HARMONIZED_TABLES_TASK
        WAREHOUSE = FRED_WH
        AFTER CREATE_VIEWS_TASK
    AS
        CALL SYSTEM$EXECUTE_PYTHON_FUNCTION(
            'steps.07_HarmonizeData',
            'harmonize_currency_data',
            ARRAY_CONSTRUCT()
        );
    """).collect()
    
    # Task 5: Update Analytics Tables
    session.sql("""
    CREATE OR REPLACE TASK UPDATE_ANALYTICS_TASK
        WAREHOUSE = FRED_WH
        AFTER LOAD_HARMONIZED_TABLES_TASK
    AS
        CALL SYSTEM$EXECUTE_PYTHON_FUNCTION(
            'steps.08_currency_exchange_analytics',
            'update_exchange_rate_analytics',
            ARRAY_CONSTRUCT()
        );
    """).collect()
    
    # Enable all tasks
    session.sql("ALTER TASK LOAD_RAW_DATA_TASK RESUME").collect()
    session.sql("ALTER TASK APPLY_CURRENCY_CONVERSION_TASK RESUME").collect()
    session.sql("ALTER TASK CREATE_VIEWS_TASK RESUME").collect()
    session.sql("ALTER TASK LOAD_HARMONIZED_TABLES_TASK RESUME").collect()
    session.sql("ALTER TASK UPDATE_ANALYTICS_TASK RESUME").collect()
    
    print("✅ All tasks created and enabled successfully!")

def main():
    # Connect to Snowflake
    try:
        session = Session.builder.configs(snowflake_params).create()
        print("✅ Snowflake connection successful!")
    except Exception as e:
        print("❌ Snowflake connection failed:", e)
        return

    # Create tasks
    create_tasks(session)

    # Close session
    session.close()
    print("🔄 Snowflake session closed.")

if __name__ == "__main__":
    main()