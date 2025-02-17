import os
import pandas as pd

#directory = "/scratch/ntc8tt/ev_project/location_data/data_from_andrew/MobileCSVFiles/201901"
#csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
#month = "201901"
#print(csv_files)
#dfs = []
#date = 1
#for file in csv_files:
   # print("reading date " + str(date))
   # file_path = os.path.join(directory, file)
   # df = pd.read_csv(file_path, low_memory=False)
   # print("finish reading dat " + str(date))
   # dfs.append(df)
   # date += 1

#final_df = pd.concat(dfs, ignore_index=True)
#final_df.to_csv(month+"_raw_data.csv", index=False)

#print(f"Combined {len(csv_files)} CSV files into a single DataFrame with {final_df.shape[0]} rows and {final_df.shape[1]} columns.")

import os
import pandas as pd

# Define the directory containing the CSV files
directory = "/scratch/ntc8tt/ev_project/location_data/data_from_andrew/MobileCSVFiles/201901"
# List all CSV files in the directory
csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
month = "201901"

# Output file to store the combined data
output_file = month+"_raw_data.csv"
date = 1
# Process files in chunks and append to a single output CSV
for i, file in enumerate(csv_files):
    file_path = os.path.join(directory, file)
    print("reading day " + str(date))
    # Read in chunks and write to CSV to prevent memory issues
    chunk_iter = pd.read_csv(file_path, chunksize=100000, low_memory=False)

    for chunk in chunk_iter:
        # Append to output CSV; write header only for the first file
        chunk.to_csv(output_file, mode="a", index=False, header=(i == 0))

    print("finish reading day " + str(date))
    date += 1

print(f"Successfully combined {len(csv_files)} CSV files into {output_file}")
