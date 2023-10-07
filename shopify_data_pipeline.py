import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {{
#no email, independent task instances
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
#wait 5 min before retrying
    'retry_delay': timedelta(minutes=5),
}}
#run this daily with no catchup as previous data is already imported
dag = DAG('shopify_linear_regression_pipeline',
          default_args=default_args,
          description='Data pipeline for Shopify data ingestion and linear regression model',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2023, 9, 2),
          catchup=False)
#ETL scripts
def run_ingestion_script():
    subprocess.run(["python", "shopify_data_ingestion.py"])

def run_transformation_script():
    subprocess.run(["python", "data_transformation.py"])

t1 = PythonOperator(
    task_id='run_ingestion_script',
    python_callable=run_ingestion_script,
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_transformation_script',
    python_callable=run_transformation_script,
    dag=dag,
)
#extract or ingest first, then transform
t1 >> t2

#transforms with SQL connection and dropping values
def transform_shopify_data():
    try:
        # Read data from SQL database
        conn = sqlite3.connect('shopify_data.db')
        df = pd.read_sql_query('SELECT * FROM shopify_customers', conn)
        
        # Perform transformations (Assuming we're keeping only 'customer_id' and 'subtotal_price' for linear regression)
        df_transformed = df[['customer_id', 'subtotal_price']]
        
        # Handle missing values
        df_transformed['subtotal_price'].fillna(0.0, inplace=True)
        
        # Save transformed data back to database
        df_transformed.to_sql('transformed_shopify_customers', conn, if_exists='replace', index=False)
        print("Successfully transformed and saved data.")
        
    except Exception as e:
        print(f"Failed to transform data. Error: {e}")

#adds this function as a task in the existing Airflow DAG


transform_task = PythonOperator(
    task_id='transform_shopify_data',
    python_callable=transform_shopify_data,
    dag=dag,
)
#basic ETL
ingestion_task >> transform_task >> analysis_task



import sqlite3

def import_csv_to_sql():
    try:
        #connect to the database
        conn = sqlite3.connect('shopify_data.db')
        
        # Read the CSV into a DataFrame
        df = pd.read_csv('test/final_customers_data.csv')
        
        # Save the DataFrame to SQL
        df.to_sql('hashed_shopify_customers', conn, if_exists='replace', index=False)
        print("Successfully imported CSV data to SQL.")
    except Exception as e:
        print(f"Failed to import CSV to SQL. Error: {e}")


csv_import_task = PythonOperator(
    task_id='import_csv_to_sql',
    python_callable=import_csv_to_sql,
    dag=dag,
)

# Update task sequence: ingestion_task -> transform_task -> csv_import_task -> analysis_task
ingestion_task >> transform_task >> csv_import_task >> analysis_task
