import pandas as pd
import numpy as np
import glob
import os
import argparse

# Define a function to calculate features
def calculate_features(file_path):
    # Load the CSV file
    df = pd.read_csv(file_path, sep='\t', header=None, names=['timestamp', 'relative_time', 'size', 'direction'])

    # Ensure proper data types
    df['timestamp'] = pd.to_numeric(df['timestamp'])
    df['relative_time'] = pd.to_numeric(df['relative_time'])
    df['size'] = pd.to_numeric(df['size'])
    df['direction'] = df['direction'].astype(int)

    # Calculate features
    df['packet_interval'] = df['timestamp'].diff().fillna(0)
    df['cumulative_size'] = df['size'].cumsum()
    # df['cumulative_size_sent'] = df[df['direction'] == 0]['size'].cumsum().fillna(0)
    # df['cumulative_size_received'] = df[df['direction'] == 1]['size'].cumsum().fillna(0)
    # Get cumulative sums for sent packets (direction == 0)
    
    sent_cum = df[df['direction'] == 0]['size'].cumsum()
    df['cumulative_size_sent'] = sent_cum.reindex(df.index).ffill().fillna(0)

    # Get cumulative sums for received packets (direction == 1)
    received_cum = df[df['direction'] == 1]['size'].cumsum()
    df['cumulative_size_received'] = received_cum.reindex(df.index).ffill().fillna(0)
    
    df['size_ratio'] = df['cumulative_size_sent'] / (df['cumulative_size_received'] + 1e-6)
    df['direction_change'] = (df['direction'] != df['direction'].shift(1)).astype(int).cumsum()

    # Rolling statistics for packet size
    df['size_rolling_mean'] = df['size'].rolling(window=10, min_periods=1).mean()
    df['size_rolling_std'] = df['size'].rolling(window=10, min_periods=1).std()

    # Time-based features
    session_duration = df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]
    df['relative_time_in_session'] = df['relative_time'] / session_duration

    # Save the updated file with features
    output_file = file_path.replace('.txt', '_features.csv')
    df.to_csv(output_file, index=False)
    print(f"Features calculated and saved to {output_file}")

# Process all files in a directory
# file_paths = glob.glob("*.txt")

# # file_paths = r"C:\Users\חנן\Desktop\אריאל אונ'\שנה ג\פרוייקט גמר\Moment"

# for file_path in file_paths:
#     calculate_features(file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate features for each TXT file in the specified directory.")
    parser.add_argument("--input", required=True, help="Path to the directory containing TXT files.")

    args = parser.parse_args()
    input_directory = args.input

    # Process all files in the directory
    for file_path in glob.glob(os.path.join(input_directory, "*.txt")):
        calculate_features(file_path)