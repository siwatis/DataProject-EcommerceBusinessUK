# UK Business E-commerce Data Analytics

## Project Overview

This project is managed to be a comprensive data analysis framework. Starting with getting raw data from sources then conduct the ETL job to clean the data and transform the data into self-design data model. The outputs would be generated data report to answer business questions and dashboards showing overview of the business metrics, break down the metrics to the units such as by-product or be-country, business health monitoring, and customer segmentation with simple RFM analysis framework.

### About the selected data source
The data is from Kaggle: "E-commerce Business Transaction" \
link: https://www.kaggle.com/datasets/gabrielramos87/an-online-shop-business \
The data is in trasaction level contains of purchased orders and cancelled orders

### What the project do
1. Data transformation to new data model [notebook.ipyhb]
   * From the raw data after extraction, transform it to tables:
        * "transactions" : unique transaction from raw data + added calculation columns
        * "customers" : unique customer from "transaction" with customers stats such as purchase stats, activeness, segmentation (RFM scoring)
    * and save them for futher analysis usage
2. Exploratory data analysis for Business [notebook.ipyhb]
    * By using python for data analysis, it will be provide various statistics analysis and customized plotting that made we know more about the existing business and generate some quick insights
3. ETL pipeline script [etl_job.py]
    * Python scipt stands for routine tasks by doing (1.) job and load the new data to GoogleSheet (✅) and GoogleBigQuery (⛔🔜)
4. Dashboard: https://lookerstudio.google.com/reporting/1c6f2acf-afb3-4574-b130-eb1fa99b29b5
    * Connect to loaded data on Google BigQuery
    * Contents:
        1. UK E-commerce Business Overview
        2. Product and Region
        3. Call-to-action: Customer Activeness
        4. Call-to-action: Cancelled Transactions
        5. Customer Segment: RFM Analysis
     
### Stack used
* Data wragnling
    * Pandas
    * DuckDB (PostgreSQL base syntax)
    * Google cloud API
* Visualization
    * Matplotlib
    * Seaborn
* Dashboard
    * BigQuery
    * Looker Studio 
