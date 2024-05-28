import os
import pandas as pd

# Folder path containing CSV files
folder_path = r"G:\29.0 Data Infrastructure & Analytics\1.0 Projects\2024.03 DLB historical data migration\!Uploaded Months\december_2023_csv\Driver"

# Output CSV file path
output_csv_path = r"G:\29.0 Data Infrastructure & Analytics\1.0 Projects\2024.03 DLB historical data migration\!Uploaded Months\december_2023_csv\combined_december_csv_apollo_driver.csv"

# Initialize an empty DataFrame to store combined data
combined_df = pd.DataFrame()

# Iterate through each CSV file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)  # Use concat instead of append

# Write the combined data to a single CSV file
combined_df.to_csv(output_csv_path, index=False)

print(f"Combined data saved to {output_csv_path}")

