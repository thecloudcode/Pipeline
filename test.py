import requests
import pandas as pd
import time

# URL of your FastAPI endpoint
url = "http://localhost:8000/data"

# List to store the data
data_list = []

# Number of rows you want to fetch (adjust as needed)
num_rows = 1000

for _ in range(num_rows):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(data)
        data_list.append(data)
    else:
        print(f"Failed to fetch data: {response.status_code}")
        break

    # Optional: Add a small delay to avoid overwhelming the server
    time.sleep(0.1)

# Create a DataFrame from the collected data
df = pd.DataFrame(data_list)

# Display the first few rows of the DataFrame
print(df.head())

# Optionally, save the DataFrame to a CSV file
# df.to_csv("collected_data.csv", index=False)