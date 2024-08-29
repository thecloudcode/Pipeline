import pyodbc
import csv
import time

conn_str = (
    "Driver={SQL Server};"
    "Server=badassh\SQLEXPRESS;"
    "Database=futurense;"
    "Trusted_Connection=yes;"
)
csv_file_path = '../../cleaned/CPSortedByDates.csv'
table_name = 'campaign_performance_live_api'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

with open(csv_file_path, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    headers = next(csvreader)

    placeholders = ','.join(['?' for _ in headers])
    insert_query = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})"

    for row in csvreader:
        time.sleep(1)
        cursor.execute(insert_query, row)
        conn.commit()

print("Data insertion completed successfully.")

delete_query = f"DELETE FROM {table_name}"
cursor.execute(delete_query)
conn.commit()

print("All data deleted from the table.")
conn.close()