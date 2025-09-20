import pandas as pd
from fredapi import Fred
import mysql.connector
from mysql.connector import Error
from io import StringIO

try:
    print("ETL Script started...")

    print("--- Starting ETL Pipeline ---")
    print("Step 1: Extracting data from FRED...")
    fred = Fred(api_key='78d8a48a37c2d055af110e42133c5e36')
    series_id = 'PAYEMS'
    df = fred.get_series(series_id)
    jobs_df = pd.DataFrame(df, columns=['value']).reset_index().rename(columns={'index': 'date'})
    print(f"Extracted {len(jobs_df)} records from FRED.")

    print("Step 2: Transforming data (calculating MoM change)...")
    jobs_df['change_pct'] = (jobs_df['value'].pct_change() * 100).round(2)
    jobs_df['change_abs'] = jobs_df['value'].diff()
    jobs_df.dropna(inplace=True)
    jobs_df = jobs_df.rename(columns={
        'value': 'total_nonfarm',
        'change_abs': 'mom_change_abs',
        'change_pct': 'mom_change_pct'
    })
    print("Transformation complete. Sample data:")
    print(jobs_df.head())

    print("Step 3: Loading transformed data into MySQL...")

    conn = mysql.connector.connect(
        host='localhost',
        database='FredETL',
        user='root',       # Change if needed
        password='1155'    # Your password
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nonfarm_payrolls (
            date DATE PRIMARY KEY,
            total_nonfarm DECIMAL(15,2),
            mom_change_abs DECIMAL(15,2),
            mom_change_pct DECIMAL(15,2)
        );
    """)
    conn.commit()

    # Insert data row by row
    insert_query = """
        REPLACE INTO nonfarm_payrolls (date, total_nonfarm, mom_change_abs, mom_change_pct)
        VALUES (%s, %s, %s, %s);
    """
    data_tuples = list(jobs_df.itertuples(index=False, name=None))
    cursor.executemany(insert_query, data_tuples)
    conn.commit()

    print(f"{cursor.rowcount} records inserted/updated successfully!")

except Error as e:
    print(f"MySQL error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")

print("--- ETL Pipeline complete! ---")
