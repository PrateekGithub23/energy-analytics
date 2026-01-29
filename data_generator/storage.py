import json
from datetime import datetime
import os


# Store sensor data in data-lake/raw/energy/date=YYYY-MM-DD/data.json
def store_sensor(sensor_data: dict) -> None:
    # Extract date from timestamp
    timestamp = sensor_data["timestamp"]
    date = timestamp.split("T")[0]  # YYYY-MM-DD

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Project root directory
    
    # Absolute data-lake path
    folder_path = os.path.join(
        base_dir,
        "data-lake",
        "raw",
        "energy",
        f"date={date}"
    )

    os.makedirs(folder_path, exist_ok=True) # Create directory if it doesn't exist

    
    # Append JSON line, one per sensor reading
    file_path = os.path.join(folder_path, "data.json")
    # open the file in append mode and write the JSON data
    with open(file_path, "a") as f:
        json.dump(sensor_data, f)
        f.write("\n")