
# new_data_transformation.py

import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def clean_data(df):
    """
    Cleans the input DataFrame by removing NaN and duplicate values.
    
    Parameters:
        df (DataFrame): The input DataFrame to be cleaned.
    
    Returns:
        DataFrame: The cleaned DataFrame.
    """
    df.dropna(subset=['Total Spent', 'Total Orders'], inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def feature_engineering(df):
    """
    Performs feature engineering on the input DataFrame.
    
    Parameters:
        df (DataFrame): The input DataFrame for feature engineering.
    
    Returns:
        DataFrame: The DataFrame with new features.
    """
    df['Spent_per_Order'] = df['Total Spent'] / df['Total Orders']
    return df

def normalize_data(df):
    """
    Normalizes 'Total Spent' and 'Total Orders' in the input DataFrame.
    
    Parameters:
        df (DataFrame): The input DataFrame to be normalized.
    
    Returns:
        DataFrame: The normalized DataFrame.
    """
    scaler = MinMaxScaler()
    df[['Total Spent', 'Total Orders']] = scaler.fit_transform(df[['Total Spent', 'Total Orders']])
    return df

if __name__ == "__main__":
    # Read the data from the CSV file
    df = pd.read_csv('customers_exportJul19.csv')
    
    # Clean the data
    cleaned_df = clean_data(df)
    
    # Perform feature engineering
    engineered_df = feature_engineering(cleaned_df)
    
    # Normalize 'Total Spent' and 'Total Orders'
    normalized_df = normalize_data(engineered_df)
    
    # Save the transformed data to a new CSV file
    normalized_df.to_csv('transformed_customers_data.csv', index=False)
