import os
import unittest
from snowflake.connector import connect
from dotenv import load_dotenv

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

class TestUSDConversionUDF(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = connect(
            account=snowflake_params["account"],
            user=snowflake_params["user"],
            password=snowflake_params["password"],
            database=snowflake_params["database"],
            schema=snowflake_params["schema"],
            warehouse=snowflake_params["warehouse"],
            role=snowflake_params["role"]
        )
        cls.cursor = cls.connection.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.cursor.close()
        cls.connection.close()

    def test_usd_conversion_udf(self):
        test_cases = [
            (0, 0),
            (1, 1),
            (2, 0.5),
            (4, 0.25),
            (0.5, 2),
            (None, None)
        ]

        for exchange_rate, expected in test_cases:
            with self.subTest(exchange_rate=exchange_rate, expected=expected):
                self.cursor.execute(f"SELECT HARMONIZED.USD_CONVERSION_UDF({exchange_rate})")
                result = self.cursor.fetchone()[0]
                self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()