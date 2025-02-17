import pandas as pd
import os
from geopy.distance import great_circle
import re 
import sys

point = (47.6101707, -122.3356454)
# month = "201904"
month = sys.argv[1]
chunk_size = 100000

input_dir = f"/scratch/ntc8tt/ev_project/location_data/data_from_andrew/MobileCSVFiles/{month}/"
output_dir = "filtered_data/station_2"
os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists

def is_within_radius(row, radius=0.2):
    if pd.isna(row['latitude']) or pd.isna(row['longitude']):
        return False  # Skip rows with missing coordinates
    location = (row['latitude'], row['longitude'])
    return great_circle(location, point).miles <= radius

for filename in os.listdir(input_dir):
    if filename.endswith(".csv"): 
        input_file = os.path.join(input_dir, filename)
        match = re.search(r"_(\w{3})(\d{2})_", filename)
        if match:
            date = match.group(2)
        else:
            print(f"Skipping file {filename}: Unable to extract day.")
            continue
        output_file = os.path.join(output_dir, f"{month}_{date}_station_1.csv")
        print(f"Processing {filename} -> Saving as {output_file}")

        if os.path.exists(output_file):
            print(f"File {output_file} already exists. Skipping this file and moving to the next.")
            continue

        with pd.read_csv(input_file, chunksize=chunk_size, low_memory=False) as reader:
            times = 1
            for i, chunk in enumerate(reader):
                print(f"Processing chunk {times}")
                filtered_chunk = chunk[chunk.apply(is_within_radius, axis=1)]
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                filtered_chunk.to_csv(output_file, mode=mode, index=False, header=header)
                times += 1

        print(f"Filtered file saved: {output_file}")

print("Processing complete!")