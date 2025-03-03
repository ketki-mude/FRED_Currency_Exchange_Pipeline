import streamlit as st
import snowflake.connector
import pandas as pd
import os
 
# Page configuration
st.set_page_config(
    page_title="FRED Currency Exchange Analytics",
    page_icon="💱",
    layout="wide"
)
 
# App title and description
st.title("💱 FRED Currency Exchange Analytics")
st.markdown("View daily and monthly currency exchange rate analytics from FRED data")
 
# Snowflake connection function
@st.cache_resource
def create_snowflake_connection():
    try:
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"]  # Ensure schema is fetched correctly
        )
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()
 
# Connect to Snowflake
try:
    conn = create_snowflake_connection()
    st.success("Connected to Snowflake successfully!")
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")
    st.stop()
 
# Data type selection
data_type = st.selectbox("Select Data Type", ["Daily", "Monthly"], index=0)
 
# Function to fetch data based on selection
def fetch_data(data_type, conn):
    if data_type == "Daily":
        query = f"""
        SELECT * FROM {st.secrets["snowflake"]["database"]}.{st.secrets["snowflake"]["schema"]}.DAILY_DATA_METRICS
        ORDER BY DDATE DESC
        LIMIT 20
        """
 
        df = pd.read_sql(query, conn)
        date_col = "DDATE"
        metrics = {
            "US-India Exchange Rate": "DEXINUS",
            "US-EU Exchange Rate": "DEXUSEU_CONVERTED",
            "US-UK Exchange Rate": "DEXUSUK_CONVERTED"
        }
    else:  # Monthly
        query = f"""
        SELECT * FROM {st.secrets["snowflake"]["database"]}.{st.secrets["snowflake"]["schema"]}.MONTHLY_DATA_METRICS
        ORDER BY MDATE DESC
        LIMIT 20
        """
        df = pd.read_sql(query, conn)
        date_col = "MDATE"
        metrics = {
            "US-India Exchange Rate": "EXINUS",
            "US-EU Exchange Rate": "EXUSEU_CONVERTED",
            "US-UK Exchange Rate": "EXUSUK_CONVERTED"
        }
   
    # Reverse the order for plotting (oldest to newest)
    df_plot = df.sort_values(by=date_col)
   
    return df, df_plot, date_col, metrics
 
# Fetch data
try:
    df, df_plot, date_col, metrics = fetch_data(data_type, conn)
   
    # Display the data table
    st.subheader(f"{data_type} Currency Exchange Data")
    st.dataframe(df, use_container_width=True)
   
    # Create visualizations using Streamlit's native charts
    st.subheader(f"{data_type} Currency Exchange Trends")
   
    # Create tabs for different visualizations
    tab1, tab2, tab3 = st.tabs(["Exchange Rates", "Rate Changes", "Volatility"])
   
    with tab1:
        # Line chart for exchange rates
        st.line_chart(
            df_plot,
            x=date_col,
            y=list(metrics.values()),
            use_container_width=True
        )
   
    with tab2:
        # Rate change percentages
        if data_type == "Daily":
            change_metrics = {
                "US-India Rate Change %": "rate_change_percent_dexinus",
                "US-EU Rate Change %": "rate_change_percent_dexuseu_converted",
                "US-UK Rate Change %": "rate_change_percent_dexusuk_converted"
            }
        else:
            change_metrics = {
                "US-India Rate Change %": "rate_change_percent_exinus",
                "US-EU Rate Change %": "rate_change_percent_exuseu_converted",
                "US-UK Rate Change %": "rate_change_percent_exusuk_converted"
            }
       
        st.bar_chart(
            df_plot,
            x=date_col,
            y=list(change_metrics.values()),
            use_container_width=True
        )
   
    with tab3:
        # Volatility metrics
        if data_type == "Daily":
            volatility_metrics = {
                "US-India Volatility": "volatility_dexinus",
                "US-EU Volatility": "volatility_dexuseu_converted",
                "US-UK Volatility": "volatility_dexusuk_converted"
            }
        else:
            volatility_metrics = {
                "US-India Volatility": "volatility_exinus",
                "US-EU Volatility": "volatility_exuseu_converted",
                "US-UK Volatility": "volatility_exusuk_converted"
            }
       
        st.area_chart(
            df_plot,
            x=date_col,
            y=list(volatility_metrics.values()),
            use_container_width=True
        )
 
except Exception as e:
    st.error(f"Error fetching data: {e}")
 
# Footer
st.markdown("---")
st.markdown("Data source: Federal Reserve Economic Data (FRED)")
 