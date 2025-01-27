"""
This script takes a text file and converts it to a CSV file.
The text file should have the following format:
Timestamp Relative time Size Direction.
It is more convenient to convert to work with the data in CSV format.
"""

import os
import argparse
import pandas as pd

def process_txt_to_csv(input_dir):
    # Ensure the directory exists
    if not os.path.isdir(input_dir):
        print(f"Directory {input_dir} does not exist.")
        return
    
    # Loop through all files in the directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            txt_file_path = os.path.join(input_dir, filename)
            csv_file_path = os.path.join(input_dir, os.path.splitext(filename)[0] + ".csv")
            
            # Read the TXT file and process it
            try:
                data = []
                with open(txt_file_path, 'r') as file:
                    for line in file:
                        # Split the line by whitespace
                        parts = line.strip().split()
                        if len(parts) == 4:
                            data.append(parts)
                
                # Convert to DataFrame
                df = pd.DataFrame(data, columns=["Timestamp", "Relative time", "Size", "Direction"])
                
                # Save to CSV
                df.to_csv(csv_file_path, index=False)
                print(f"Processed {filename} -> {os.path.basename(csv_file_path)}")
            except Exception as e:
                print(f"Failed to process {filename}: {e}")

# Usage example
# input_directory = "path_to_directory"
# process_txt_to_csv(input_directory)

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input', type=str, required=True, help='Path to the directory containing txt files')
    # args = parser.parse_args()

    # process_txt_to_csv(args.input)

    # Create the parser    
    parser = argparse.ArgumentParser(description="Convert TXT files to CSV in the specified directory.")
    parser.add_argument("--input", required=True, help="Path to the directory containing TXT files.")
    
    # Parse arguments
    args = parser.parse_args()
    input_directory = args.input

    # Process the directory
    process_txt_to_csv(input_directory)
