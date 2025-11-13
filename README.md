# Capstone_project

This repository contains the capstone project to build an end-to-end data pipeline and dashboards for sales analytics.

### Scope & Objectives
- Ingest raw sales events (CSV)
- Clean and transform data for analytics
- Build ETL orchestration with Airflow
- Use Spark for big-data processing
- Deploy processed data to cloud storage
- Create dashboards (Power BI / Tableau)

### tasks completed
- Project structure created
- Synthetic dataset generated: `data/raw/sales_data.csv`
- Data dictionary added: `docs/data_dictionary.md`
- Initial EDA and small cleaned sample saved: `data/processed/sample_clean.csv`
- Next: implement `src/data_cleaning.py` for thorough cleaning 


# Data Cleaning & Validation
- Added `src/data_cleaning.py` script.
- Removed duplicates, fixed data types, handled nulls.
- Added computed fields (margin %, discount).
- Exported cleaned data in CSV + Parquet formats.
  
- Next: Implement ETL with Airflow

# Capstone ETL Pipeline with Apache Airflow

This project implements a simple **ETL workflow** using Apache Airflow.

##  Overview
- Extract sales data from CSV  
- Transform data (add total sales column)  
- Load into final CSV output  

## Tools
- **Apache Airflow** (workflow orchestration)
- **Pandas** (data processing)
- **Docker Compose** (Airflow environment)

##  DAG Workflow
`extract_data` → `transform_data` → `load_data`
