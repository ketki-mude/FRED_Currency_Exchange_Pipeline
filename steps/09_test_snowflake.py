import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import os

@pytest.fixture(scope="module")
def snowflake_session():
    # Replace with your actual Snowflake connection parameters
    snowflake_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "database": "FRED_DB",
        "schema": "HARMONIZED",
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    session = Session.builder.configs(snowflake_params).create()
    yield session
    session.close()

def test_usd_conversion_udf_valid_exchange_rate(snowflake_session):
    exchange_rate = 2
    result = snowflake_session.sql(f"SELECT HARMONIZED.USD_CONVERSION_UDF({exchange_rate})").collect()[0][0]
    assert result == 0.5, f"Expected 0.5 but got {result}"

def test_usd_conversion_udf_zero_exchange_rate(snowflake_session):
    exchange_rate = 0
    result = snowflake_session.sql(f"SELECT HARMONIZED.USD_CONVERSION_UDF({exchange_rate})").collect()[0][0]
    assert result == 0, f"Expected 0 but got {result}"

def test_usd_conversion_udf_null_exchange_rate(snowflake_session):
    result = snowflake_session.sql("SELECT HARMONIZED.USD_CONVERSION_UDF(NULL)").collect()[0][0]
    assert result is None, f"Expected None but got {result}"

def test_update_data_metrics_dummy(snowflake_session):
    # Switch to the DEV_HARMONIZED_SCHEMA (if not already set in connection)
    snowflake_session.sql("USE SCHEMA DEV_HARMONIZED_SCHEMA").collect()

    # Call the stored procedure from DEV_HARMONIZED_SCHEMA
    sp_result = snowflake_session.sql("CALL DEV_HARMONIZED_SCHEMA.UPDATE_DATA_METRICS()").collect()[0][0]
    print("Stored procedure returned:", sp_result)

    # Now, to mimic and print conversion outputs, execute a dummy query with inline values.
    # This does not rely on any table but shows how your conversion output might be represented.
    dummy_query = """
      SELECT 
        TO_DATE('2025-02-01') AS DDATE,
        10.0 AS rate_change_percent_dexinus,
        0.5 AS rate_change_percent_dexuseu_converted,
        1.2 AS rate_change_percent_dexusuk_converted
    """
    dummy_results = snowflake_session.sql(dummy_query).collect()
    print("Dummy conversion results:")
    for row in dummy_results:
        print(row)

if __name__ == "__main__":
    import pytest
    pytest.main()