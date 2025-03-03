--!jinja
 
-- Create schemas based on environment
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_RAW_SCHEMA";
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_HARMONIZED_SCHEMA";
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_ANALYTICS_SCHEMA";
 
-- Deploy load_raw_data notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."INTEGRATIONS"."{{env}}_load_raw_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/load_raw_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'load_raw_data.ipynb';
 
ALTER NOTEBOOK "FRED_DB"."INTEGRATIONS"."{{env}}_load_raw_data" ADD LIVE VERSION FROM LAST;
 
-- Deploy harmonize_data notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."INTEGRATIONS"."{{env}}_harmonize_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/harmonized_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'harmonize_data.ipynb';
 
ALTER NOTEBOOK "FRED_DB"."INTEGRATIONS"."{{env}}_harmonize_data" ADD LIVE VERSION FROM LAST;
 
-- Deploy analytics notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."INTEGRATIONS"."{{env}}_analytics"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/analytics/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'analytics.ipynb';
 
ALTER NOTEBOOK "FRED_DB"."INTEGRATIONS"."{{env}}_analytics" ADD LIVE VERSION FROM LAST;
 