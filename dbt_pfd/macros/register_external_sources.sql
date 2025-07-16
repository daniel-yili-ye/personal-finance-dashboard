-- macros/register_external_sources.sql
{% macro register_external_sources() %}
  {% if execute %}
    -- Create schemas for external sources
    CREATE SCHEMA IF NOT EXISTS amex;
    CREATE SCHEMA IF NOT EXISTS wealthsimple;
    CREATE SCHEMA IF NOT EXISTS reference_data;
    
    -- Register AmEx transactions external view
    CREATE OR REPLACE VIEW amex.transactions AS
    SELECT *
    FROM read_parquet('{{ var("project_root") }}/data/processed/american_express/transactions/**/*.parquet',
                      hive_partitioning := TRUE);
    
    -- Register WealthSimple transactions external view
    CREATE OR REPLACE VIEW wealthsimple.transactions AS
    SELECT *
    FROM read_parquet('{{ var("project_root") }}/data/processed/wealthsimple/transactions/**/*.parquet',
                      hive_partitioning := TRUE);
    
    -- Register categories reference data
    CREATE OR REPLACE VIEW reference_data.categories AS
    SELECT *
    FROM read_csv_auto('{{ var("project_root") }}/data/raw/categories.csv');
  {% endif %}
{% endmacro %} 