import warnings
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
import os
import pandas as pd
import duckdb
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
load_dotenv()

def etl_job(data_path):

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    cred_path = os.getenv('CRED_PATH') # your credential file path
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    client = gspread.authorize(creds)

    sheet_id = os.getenv('SHEET_ID') # your GoogleSheet ID
    workbook = client.open_by_key(sheet_id)

    transactions_sheet = workbook.worksheet('transactions')
    customers_sheet = workbook.worksheet('customers')

    print('Connect GoogleSheetAPI Completed')

    transactions = pd.read_csv(data_path)
    transactions.columns = [col_name.lower().replace('no','_no').replace('name','_name') for col_name in transactions.columns]
    transactions['date'] = pd.to_datetime(transactions['date'])
    transactions['value'] = transactions['price'] * transactions['quantity']
    transactions['fee'] = transactions['value'] * 0.02 # suppose fee is 2%

    scores = ['recency_score','frequency_score','monetery_score']
    customers = duckdb.sql("""
        --sql
        SELECT *, 
            total_purchase / life_time AS frequency,
            (PERCENT_RANK() OVER (ORDER BY recency DESC)) AS recency_pct,
            (PERCENT_RANK() OVER (ORDER BY total_purchase / life_time)) AS frequency_pct,
            (PERCENT_RANK() OVER (ORDER BY total_spend)) AS monetery_pct,
            (total_spend - COALESCE(total_cancel,0)) / (life_time/30) AS cltv,
        FROM (
            SELECT customer_no, country,
                MIN(date) AS first_purchase_date,
                MAX(date) AS last_purchase_date,
                COUNT(DISTINCT product_no) AS total_product,
                COUNT(*) AS total_purchase,
                COUNT(DISTINCT DATETRUNC('MONTH', date)) AS month_active,
                COUNT(DISTINCT DATETRUNC('DAY', date)) AS day_active,        
                AVG(CASE WHEN value >= 0 THEN value END) AS avg_basket_size,
                SUM(CASE WHEN value >= 0 THEN value END) AS total_spend,
                SUM(CASE WHEN value < 0 THEN value *-1 END) AS total_cancel,
                DATE_PART('day', (SELECT MAX(date) FROM transactions) - MAX(date)) AS recency,
                DATE_PART('day', (SELECT MAX(date) FROM transactions) - MIN(date)) AS life_time
            FROM transactions
            GROUP BY customer_no, country
        ) a
        ;
    """).df() \
        .assign(recency_score = lambda df: pd.qcut(df['recency_pct'], 5, labels=[1,2,3,4,5]).astype(int),
                frequency_score = lambda df: pd.qcut(df['frequency_pct'], 5, labels=[1,2,3,4,5]).astype(int),
                monetery_score = lambda df: pd.qcut(df['monetery_pct'], 5, labels=[1,2,3,4,5]).astype(int),
                average_score = lambda df: df[scores].mean(axis=1)) \
        .drop(columns=['recency_pct','frequency_pct','monetery_pct'])
    
    print('Data Loaded and Transformed')

    transactions_sheet.clear()
    customers_sheet.clear()
    set_with_dataframe(transactions_sheet, transactions)
    print('Updated transactions')
    set_with_dataframe(customers_sheet, customers)
    print('Updated customers')

    print('GoogleSheet Updated Complete')

if __name__ == '__main__':
    etl_job('Sales Transaction v.4a.csv')

    

