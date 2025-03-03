from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from dotenv import load_dotenv
import os
from snowflake.snowpark.functions import col, min, max
from snowflake.snowpark.window import Window
from copy import copy

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

raw_data = {
    "RAW_DAILY": ["RAW_DEXINUS", "RAW_DEXUSEU", "RAW_DEXUSUK"],
    "RAW_MONTHLY": ["RAW_EXINUS", "RAW_EXUSEU", "RAW_EXUSUK"]
}

def fill_missing_dates(session, df):
    # Sort the dataframe by date
    df = df.sort(col('"date"'))
    
    # Get min and max date from the dataset
    date_range_df = df.agg(min(col('"date"')).alias("MIN_DATE"), max(col('"date"')).alias("MAX_DATE")).collect()
    min_date, max_date = date_range_df[0]["MIN_DATE"], date_range_df[0]["MAX_DATE"]
    print(min_date, max_date)

    # Calculate the number of days between min_date and max_date
    date_diff_query = f"""
        SELECT DATEDIFF(DAY, '{min_date}', '{max_date}') AS date_diff
    """
    date_diff_df = session.sql(date_diff_query).collect()
    date_diff = date_diff_df[0]["DATE_DIFF"]

    # Generate full date range using Snowflake's GENERATOR function
    full_date_range_df = session.sql(f"""
        SELECT CAST(DATEADD(DAY, SEQ4(), '{min_date}') AS DATE) AS date
        FROM TABLE(GENERATOR(ROWCOUNT => {date_diff + 1}))
    """)

    # Perform left join to get missing dates and fill the missing ones with the last known value
    df_with_full_dates = full_date_range_df.join(df, on=full_date_range_df["DATE"] == df['"date"'], how="left")
    #df_with_full_dates.show(15)

   # Define window specification
    window_spec = Window.order_by("DATE").rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)

    # Apply the LAG function to fill missing values using the window spec
    df_with_full_dates = df_with_full_dates.with_column(
        '"value"',
        F.coalesce(
            col('"value"'),
            F.last_value('"value"', True).over(window_spec)
        )
    )

    # Select only the "DATE" and "value" columns
    df_with_full_dates = df_with_full_dates.select(col("DATE").alias("DATE"), col('"value"'))

    # Final DataFrame sorted by date
    final_df = df_with_full_dates.sort(col("DATE"))
    return final_df

def create_harmonized_view(session, raw_data):
    session.use_schema("HARMONIZED")

    for schema_name, tables in raw_data.items():
        base_df = None  # Initialize base DataFrame

        for table in tables:
            table_suffix = table.replace("RAW_", "")  # Extract suffix (e.g., exinus)
            df = session.table(f"{schema_name}.{table}").select(F.col('"date"'), F.col('"value"'))

            # Fill missing dates
            if schema_name == "RAW_DAILY":
                df = fill_missing_dates(session, df).select(F.col("DATE").alias("DDATE"), F.col('"value"').alias(table_suffix))
                # Create a copy of df for the join to avoid self-join issue
                df_copy = copy(df)
                base_df = df_copy if base_df is None else base_df.join(df_copy, on="DDATE", how="outer")
            else:
                df = df.select(F.col('"date"').alias("MDATE"), F.col('"value"').alias(table_suffix))
                # Create a copy of df for the join to avoid self-join issue
                df_copy = copy(df)
                base_df = df_copy if base_df is None else base_df.join(df_copy, on="MDATE", how="outer")

        # Apply UDF to specific columns
        if schema_name == "RAW_DAILY":
            base_df = base_df.with_column("DEXUSEU_CONVERTED", F.call_udf("HARMONIZED.USD_CONVERSION_UDF", F.col("DEXUSEU")))
            base_df = base_df.with_column("DEXUSUK_CONVERTED", F.call_udf("HARMONIZED.USD_CONVERSION_UDF", F.col("DEXUSUK")))
        else:
            base_df = base_df.with_column("EXUSEU_CONVERTED", F.call_udf("HARMONIZED.USD_CONVERSION_UDF", F.col("EXUSEU")))
            base_df = base_df.with_column("EXUSUK_CONVERTED", F.call_udf("HARMONIZED.USD_CONVERSION_UDF", F.col("EXUSUK")))

        # Create view for daily and monthly separately
        view_name = f"HARMONIZED_{schema_name.split('_')[1]}_V".upper()
        base_df.create_or_replace_view(view_name)
        print(f"✅ {view_name} created successfully!")

def create_harmonized_stream(session, raw_data):
    session.use_schema('HARMONIZED')
    
    for schema_name in raw_data.keys():
        if schema_name == "RAW_DAILY":
            # Materialize the view into a table
            session.sql("""
                CREATE OR REPLACE TABLE HARMONIZED_DAILY_TBL AS 
                SELECT * FROM HARMONIZED_DAILY_V
            """).collect()
            
            # Create a stream on the materialized table
            _ = session.sql("""
                CREATE OR REPLACE STREAM HARMONIZED_DAILY_STREAM 
                ON TABLE HARMONIZED_DAILY_TBL 
                SHOW_INITIAL_ROWS = TRUE
            """).collect()
            print("Daily Stream created successfully!")
        
        else:
            # Materialize the view into a table
            session.sql("""
                CREATE OR REPLACE TABLE HARMONIZED_MONTHLY_TBL AS 
                SELECT * FROM HARMONIZED_MONTHLY_V
            """).collect()
            
            # Create a stream on the materialized table
            _ = session.sql("""
                CREATE OR REPLACE STREAM HARMONIZED_MONTHLY_STREAM 
                ON TABLE HARMONIZED_MONTHLY_TBL 
                SHOW_INITIAL_ROWS = TRUE
            """).collect()
            print("Monthly Stream created successfully!")

try:
    session = Session.builder.configs(snowflake_params).create()
    print("✅ Snowflake connection successful!")
    create_harmonized_view(session, raw_data)
    create_harmonized_stream(session, raw_data)
    session.close()
    print("🔄 Snowflake session closed.")
except Exception as e:
    print("❌ Snowflake connection failed:", e)
    exit()
