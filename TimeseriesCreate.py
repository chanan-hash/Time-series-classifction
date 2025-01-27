import pandas as pd
import numpy as np
from collections import Counter
import argparse

def process_quic_traffic(input_file, output_file):
    """
    Process QUIC traffic data and create an aggregated CSV file with per-second statistics.
    
    Parameters:
    input_file (str): Path to the input CSV file
    output_file (str): Path where the output CSV will be saved
    
    Returns:
    pd.DataFrame: The processed dataframe that was saved to CSV
    """
    # Read the original CSV file
    df = pd.read_csv(input_file)
    
    # Convert timestamp to integer seconds
    df['second'] = df['Timestamp'].astype(int)
    
    # Calculate time from start
    start_time = df['second'].min()
    df['time_from_start'] = df['second'] - start_time
    
    # Group by second and calculate aggregated statistics
    aggregated_stats = []
    
    for second, group in df.groupby('second'):
        # Basic statistics
        total_size = group['Size'].sum()
        packets_count = len(group)
        avg_size = group['Size'].mean()
        
        # Find most common direction
        direction_counter = Counter(group['Direction'])
        most_common_direction = direction_counter.most_common(1)[0][0]
        
        # Direction ratios
        direction_0_count = direction_counter.get(0, 0)
        direction_1_count = direction_counter.get(1, 0)
        
        stats = {
            'second': second,
            'time_from_start': second - start_time,
            'total_size': total_size,
            'packets_count': packets_count,
            'average_size': avg_size,
            'most_common_direction': most_common_direction,
            'packets_per_second': packets_count,
            'direction_0_ratio': direction_0_count / packets_count,
            'direction_1_ratio': direction_1_count / packets_count
        }
        
        aggregated_stats.append(stats)
    
    # Create DataFrame from aggregated statistics
    result_df = pd.DataFrame(aggregated_stats)
    
    # Sort by second
    result_df = result_df.sort_values('second').reset_index(drop=True)
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    
    # Print some summary statistics
    print(f"Processed {len(df)} packets into {len(result_df)} seconds of data")
    print(f"\nTime range: {result_df['second'].min()} to {result_df['second'].max()}")
    print(f"Total duration: {len(result_df)} seconds")
    print(f"\nAverage packets per second: {result_df['packets_count'].mean():.2f}")
    print(f"Max packets in one second: {result_df['packets_count'].max()}")
    print(f"Min packets in one second: {result_df['packets_count'].min()}")
    
    return result_df

# Example usage
if __name__ == "__main__":
    # input_file = "GoogleDoc31.csv"
    # output_file = "quic_traffic_aggregated.csv"

    parser = argparse.ArgumentParser(description="Process QUIC traffic data and create an aggregated CSV file with per-second statistics.")
    parser.add_argument("--input", required=True, help="Path to the input CSV file")
    parser.add_argument("--output", required=True, help="Path where the output CSV will be saved")
    args = parser.parse_args()
    input_file = args.input
    output_file = args.output
    
    # Process the data
    aggregated_df = process_quic_traffic(input_file, output_file)
    
    # Display the first few rows of the processed data
    print("\nFirst few rows of the processed data:")
    print(aggregated_df.head())