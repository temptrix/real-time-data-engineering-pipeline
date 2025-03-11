import requests
import json
import time
import os

# Set up storage directory (simulating a Data Lake)
DATA_LAKE_PATH = "data_lake/raw_data"
os.makedirs(DATA_LAKE_PATH, exist_ok=True)

# API URL - Fetching Bitcoin price every 10 seconds
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"

def fetch_data():
    """Fetch real-time cryptocurrency prices and store them in the data lake."""
    while True:
        try:
            response = requests.get(API_URL)
            if response.status_code == 200:
                data = response.json()
                timestamp = int(time.time())

                # Save data as a JSON file
                file_path = f"{DATA_LAKE_PATH}/crypto_data_{timestamp}.json"
                with open(file_path, "w") as file:
                    json.dump(data, file, indent=4)
                
                print(f"Data saved: {file_path}")

            else:
                print(f"Error fetching data: {response.status_code}")

        except Exception as e:
            print(f"Error: {e}")

        # Fetch new data every 10 seconds
        time.sleep(10)

if __name__ == "__main__":
    fetch_data()
