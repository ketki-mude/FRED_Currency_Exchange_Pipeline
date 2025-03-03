# FRED_Currency_Exchange

# **FRED Currency Exchange Processing System**
This project automates the ingestion, transformation, and validation of currency exchange rate data from **FRED** using **Snowflake and Streamlit**. It ensures seamless data processing, integrity checks, and interactive financial data visualization.

---
## **📌 Project Resources**
- 📘 **Google Codelab:** [CodeLab](https://codelabs.example.com/fred-currency-exchange)
- 🌐 **App (Streamlit Cloud):** [Streamlit Link](https://fred-currency-exchange.streamlit.app/)
- 🎥 **YouTube Demo:** [YouTube Video](https://youtu.be/your-demo-video)

---
## **📌 Technologies Used**
![Snowflake](https://img.shields.io/badge/-Snowflake-56CCF2?style=for-the-badge&logo=snowflake&logoColor=white)  
![Streamlit](https://img.shields.io/badge/-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)  
![AWS S3](https://img.shields.io/badge/-AWS_S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)

---
## **📌 Architecture Diagram**
<p align="center">
  <img src="https://github.com/yourusername/yourrepo/blob/main/architecture-diagram/fred_architecture_diagram.jpg" 
       alt="Architecture Diagram" width="600" height="600">
</p>

---
## **📌 Project Flow**

### **Step 1: Dashboard & Environment Selection**
- Users are presented with an interactive **Streamlit dashboard**.
- On the dashboard, users can select the environment: **DEV** or **PROD**.
- This selection determines the configuration and data views they access from Snowflake.

### **Step 2: Data Pipeline Execution**
- **Multiple automated pipelines** operate in parallel:
  - **Raw Pipeline:** Ingests and stages raw JSON data from the FRED API.
  - **Harmonization Pipeline:** Transforms raw data into a structured, cleaned format using Snowpark Python.
  - **Analytics Pipeline:** Aggregates precomputed metrics for daily, weekly, and monthly performance.
- Each pipeline ensures robust and structured data processing and seamless ingestion into Snowflake.

### **Step 3: Fetching & Displaying Data**
- Based on the selected environment, the dashboard displays the corresponding processed data:
  - **Raw Data:** Directly from the raw ingestion pipeline.
  - **Harmonized Data:** From the refined, cleaned data pipeline.
  - **Analytics Data:** Precomputed metrics are showcased via tables and interactive graphs.
- The dashboard enables users to explore data insights without writing SQL queries.

### **Step 4: Additional Data Processing**
- Further data transformations are applied:
  - **Harmonized Data:** Ensures consistency with standardized timestamps and normalized currency rates.
  - **Analytics Data:** Enriched with volatility metrics and performance indicators.
- These datasets are updated regularly via automated Snowflake tasks and integrated CI/CD pipelines.

---
## **📌 Attestation**
**WE CERTIFY THAT WE HAVE NOT USED ANY OTHER STUDENTS' WORK IN OUR ASSIGNMENT AND COMPLY WITH THE POLICIES OUTLINED IN THE STUDENT HANDBOOK.**
