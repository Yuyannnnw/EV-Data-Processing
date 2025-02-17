import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

def count_entries(file_path):
    try:
        df = pd.read_csv(file_path)
        file_name = os.path.basename(file_path)
        key = file_name.split('_station')[0]
        return key, df["advertiser_id"].nunique()
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main(directory, output_file):
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    
    results = []
    with ProcessPoolExecutor() as executor:
        for result in executor.map(count_entries, csv_files):
            if result:
                results.append(result)
    
    df_results = pd.DataFrame(results, columns=["File Key", "Entry Count"])
    df_results.sort_values(by="File Key", inplace=True)
    df_results.to_csv(output_file, index=False)

if __name__ == "__main__":
    directory = "/scratch/ntc8tt/ev_project/filtered_data/station_1"
    output_file = "s1_entry_num.csv"
    main(directory, output_file)