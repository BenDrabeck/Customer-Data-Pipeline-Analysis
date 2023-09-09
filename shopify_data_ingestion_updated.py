
import requests
# Importing required modules for data ingestion
import pandas as pd
import sqlite3
# Function to save DataFrame to SQLite database

def save_to_database(df, table_name, db_name='shopify_data.db'):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
# Handle exceptions if database save operation fails
        print(f"Successfully saved data to {table_name} in {db_name}.")
    except Exception as e:
        print(f"Failed to save data to database. Error: {e}")

# Function to fetch data from Shopify and save it to database
def fetch_data_from_shopify(api_key, password, shop_url, resource):
    # Make the API request
    response = requests.get(f"{shop_url}{resource}", auth=(api_key, password))
    
    # Check for successful request
    if response.status_code == 200:
        # Load data into a Pandas DataFrame
        data = pd.DataFrame(response.json()[resource[:-5]])  # Remove '.json' from resource to get the key
        # Save data to database
        save_to_database(data, 'shopify_customers')
        return data
    else:
        print("Failed to fetch data from Shopify.")
        return None
