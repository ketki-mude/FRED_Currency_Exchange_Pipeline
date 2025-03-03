from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from dotenv import load_dotenv
import os

from steps.06_forward_fill_udf.function import forward_fill_udf
from snowflake.snowpark.functions import flatten, last

load_dotenv()

snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": "FRED_DB",
    "schema": "HARMONIZED",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

def harmonize_currency_data(session):
    """
    Harmonizes daily and monthly currency exchange data into a single table.
    """
    # Load daily and monthly data for INRUSD, EURUSD, GBPUSD
    inrusd = session.table("RAW_DAILY.DEXINUS").select("date", "value").unionAll(
        session.table("RAW_MONTHLY.EXINUS").select("date", "value")
    )
    
    eurusd = session.table("RAW_DAILY.DEXUSEU").select("date", "usd_to_euro").unionAll(
        session.table("RAW_MONTHLY.EXUSEU").select("date", "usd_to_euro")
    )
    
    gbpusd = session.table("RAW_DAILY.DEXUSUK").select("date", "usd_to_pound").unionAll(
        session.table("RAW_MONTHLY.EXUSUK").select("date", "usd_to_pound")
    )

    # Apply the UDF to handle missing values
    # Define window specification (order by date)
    # Forward fill for INR/USD
    inrusd = inrusd.groupBy("date").agg(
        forward_fill_udf(col("value")).alias("USDINR")
    )
    inrusd = inrusd.withColumn("USDINR", flatten(col("USDINR")))

    # Aggregate EUR/USD values into an array
    eurusd = eurusd.groupBy().agg(
        array_agg(col("usd_to_euro")).alias("eurusd_array")
    )

    # Apply the forward fill UDF
    eurusd = eurusd.withColumn("USDEUR", call_udf("FORWARD_FILL_UDF", col("eurusd_array")))

    # Flatten the results back to individual rows
    eurusd = eurusd.withColumn("USDEUR", flatten(col("USDEUR")))


    # Aggregate GBP/USD values into an array
    gbpusd = gbpusd.groupBy().agg(
        array_agg(col("usd_to_pound")).alias("gbpusd_array")
    )

    # Apply the forward fill UDF
    gbpusd = gbpusd.withColumn("USDGBP", call_udf("FORWARD_FILL_UDF", col("gbpusd_array")))

    # Flatten the results back to individual rows
    gbpusd = gbpusd.withColumn("USDGBP", flatten(col("USDGBP")))

    # Join the dataframes on the date column
    harmonized_df = inrusd.join(eurusd, "date", "outer").join(gbpusd, "date", "outer")

    # Create the harmonized table
    session.sql("CREATE OR REPLACE TABLE HARMONIZED.HARMONIZED_CURRENCY_EXCHANGE (date STRING, USDINR FLOAT, USDEUR FLOAT, USDGBP FLOAT)")

    # Insert harmonized data into the table
    harmonized_df.write.mode("append").saveAsTable("HARMONIZED.HARMONIZED_CURRENCY_EXCHANGE")

    print("✅ Harmonized currency data created successfully!")
    
# Connect to Snowflake
try:
    session = Session.builder.configs(snowflake_params).create()
    print("Snowflake connection successful!")
except Exception as e:
    print("Snowflake connection failed:", e)
    exit()

harmonize_currency_data(session)