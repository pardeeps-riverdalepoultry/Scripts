import pandas as pd
import os
import glob

def convert_parquet_to_csv(parquet_file_path, csv_file_path):
    try:
        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(parquet_file_path)
        
        # Write the DataFrame to a CSV file
        df.to_csv(csv_file_path, index=False)
        print(f"Successfully converted {parquet_file_path} to {csv_file_path}")
    except Exception as e:
        print(f"Error converting file: {e}")

if __name__ == '__main__':
    input_dir = 'C:\\Scripts\\parquetToCSV\\inputParquet'
    output_dir = 'C:\\Scripts\\parquetToCSV\\outputCSV'

    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # Get list of all Parquet files in the input directory
    parquet_files = glob.glob(os.path.join(input_dir, '*.parquet'))

    # Convert each Parquet file to CSV
    for parquet_file in parquet_files:
        # Generate the output CSV file path
        file_name = os.path.basename(parquet_file)
        csv_file_path = os.path.join(output_dir, file_name.replace('.parquet', '.csv'))

        # Convert the Parquet file to CSV
        convert_parquet_to_csv(parquet_file, csv_file_path)
