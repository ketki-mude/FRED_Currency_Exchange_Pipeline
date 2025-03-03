from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import ArrayType, FloatType
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
    "database": "FRED_DB",
    "schema": "HARMONIZED",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# Define forward fill function
def forward_fill(values):
    filled_values = []
    last_valid = None

    for value in values:
        if value is not None and not (isinstance(value, (int, float)) and value == 0):
            last_valid = value  # Update last valid value if it's not NULL or 0
        filled_values.append(last_valid)  # Fill with last valid value or None if none exists

    return filled_values


def main():
    try:
        session = Session.builder.configs(snowflake_params).create()
        print("✅ Snowflake connection successful!")
    except Exception as e:
        print("❌ Snowflake connection failed:", e)
        return

    session.sql("USE SCHEMA HARMONIZED").collect()

    # Create a stage if it doesn't exist
    session.sql("CREATE STAGE IF NOT EXISTS my_stage").collect()

    # Register UDF
    session.udf.register(
        func=forward_fill,
        return_type=ArrayType(FloatType()),
        input_types=[ArrayType(FloatType())],  # Accept an array of floats
        name="FORWARD_FILL_UDF",
        is_permanent=True,
        replace=True,
        stage_location="@my_stage"
    )

    session.close()
    print("🔄 Snowflake session closed.")

if __name__ == "__main__":
    main()
