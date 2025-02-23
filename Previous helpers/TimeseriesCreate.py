import pandas as pd
import numpy as np
from collections import Counter
import argparse


# def process_quic_traffic(input_file, output_file):
#     """
#     Process QUIC traffic data and create an aggregated CSV file with half-second statistics.
#     Each second is split into two half-second intervals.
    
#     Parameters:
#     input_file (str): Path to the input CSV file
#     output_file (str): Path where the output CSV will be saved
    
#     Returns:
#     pd.DataFrame: The processed dataframe that was saved to CSV
#     """
#     # Read the original CSV file
#     df = pd.read_csv(input_file)
    
#     # Calculate the half-second indicator (0 for first half, 1 for second half)
#     df['second'] = df['Timestamp'].astype(int)
#     df['half_second'] = ((df['Timestamp'] % 1) >= 0.5).astype(int)
    
#     # Calculate time from start
#     start_time = df['second'].min()
#     df['time_from_start'] = df['second'] - start_time
    
#     # Group by second and half-second
#     aggregated_stats = []
    
#     for (second, half), group in df.groupby(['second', 'half_second']):
#         # Basic statistics
#         total_size = group['Size'].sum()
#         packets_count = len(group)
#         avg_size = group['Size'].mean()
        
#         # Find most common direction
#         direction_counter = Counter(group['Direction'])
#         most_common_direction = direction_counter.most_common(1)[0][0]
        
#         # Direction ratios
#         direction_0_count = direction_counter.get(0, 0)
#         direction_1_count = direction_counter.get(1, 0)
        
#         # Calculate exact time for this half-second
#         half_second_time = second + (0.5 if half else 0)
#         time_from_start = half_second_time - start_time
        
#         stats = {
#             'second': second,
#             'half_second': half,  # 0 for first half, 1 for second half
#             'exact_time': half_second_time,
#             'time_from_start': time_from_start,
#             'total_size': total_size,
#             'packets_count': packets_count,
#             'average_size': avg_size,
#             'most_common_direction': most_common_direction,
#             'packets_per_half_second': packets_count,
#             'direction_0_ratio': direction_0_count / packets_count if packets_count > 0 else 0,
#             'direction_1_ratio': direction_1_count / packets_count if packets_count > 0 else 0,
#             'packets_per_second': packets_count * 2  # Normalized to per-second rate
#         }
        
#         aggregated_stats.append(stats)
    
#     # Create DataFrame from aggregated statistics
#     result_df = pd.DataFrame(aggregated_stats)
    
#     # Sort by time
#     result_df = result_df.sort_values(['second', 'half_second']).reset_index(drop=True)
    
#     # Save to CSV
#     result_df.to_csv(output_file, index=False)
    
#     # Print some summary statistics
#     total_intervals = len(result_df)
#     print(f"Processed {len(df)} packets into {total_intervals} half-second intervals")
#     print(f"\nTime range: {result_df['second'].min()} to {result_df['second'].max()}")
#     print(f"Total duration: {(result_df['second'].max() - result_df['second'].min() + 0.5):.1f} seconds")
#     print(f"\nAverage packets per half-second: {result_df['packets_count'].mean():.2f}")
#     print(f"Max packets in one half-second: {result_df['packets_count'].max()}")
#     print(f"Min packets in one half-second: {result_df['packets_count'].min()}")
    
#     # Print example of how the data is split
#     print("\nExample of half-second splitting:")
#     print(result_df[['second', 'half_second', 'packets_count', 'total_size']].head(6))
    
#     return result_df

# # Example usage
# if __name__ == "__main__":
#     # input_file = "GoogleDoc31.csv"
#     # output_file = "quic_traffic_aggregated.csv"

#     parser = argparse.ArgumentParser(description="Process QUIC traffic data and create an aggregated CSV file with per-second statistics.")
#     parser.add_argument("--input", required=True, help="Path to the input CSV file")
#     parser.add_argument("--output", required=True, help="Path where the output CSV will be saved")
#     args = parser.parse_args()
#     input_file = args.input
#     output_file = args.output
    
#     # Process the data
#     aggregated_df = process_quic_traffic(input_file, output_file)
    
#     # Display the first few rows of the processed data
#     print("\nFirst few rows of the processed data:")
#     print(aggregated_df.head())

# import pandas as pd
# import numpy as np
# from collections import Counter
# import argparse


# def process_quic_traffic(input_file, output_file, window_size_ms=1):
#     """
#     Process QUIC traffic data and create an aggregated CSV file with millisecond window statistics.
    
#     Parameters:
#     input_file (str): Path to the input CSV file
#     output_file (str): Path where the output CSV will be saved
#     window_size_ms (int): Size of the window in milliseconds (default: 1ms)
    
#     Returns:
#     pd.DataFrame: The processed dataframe that was saved to CSV
#     """
#     # Read the original CSV file
#     df = pd.read_csv(input_file)
    
#     # Convert timestamps to milliseconds with higher precision
#     df['ms_timestamp'] = (df['Timestamp'] * 1000)
    
#     # Create windows while preserving fractional milliseconds
#     window_size = window_size_ms / 1000  # Convert to seconds for calculation
#     df['window_start'] = (df['Timestamp'] // window_size) * window_size
#     df['window_end'] = df['window_start'] + window_size
    
#     # Calculate absolute millisecond from timestamp origin (epoch)
#     df['ms_from_origin'] = (df['window_start'] * 1000).astype(int)
    
#     # Convert window times to milliseconds for output
#     df['window_start_ms'] = (df['window_start'] * 1000).astype(int)
#     df['window_end_ms'] = (df['window_end'] * 1000).astype(int)
    
#     # Calculate time from start for each window
#     start_time = df['window_start'].min()
#     df['time_from_start'] = df['window_start'] - start_time
    
#     # Print diagnostic information
#     print("Timestamp range before grouping:")
#     print(f"Min timestamp: {df['Timestamp'].min()}")
#     print(f"Max timestamp: {df['Timestamp'].max()}")
#     print(f"Number of unique windows: {df['window_start'].nunique()}")
    
#     # Group by window start time
#     aggregated_stats = []
    
#     for window_start, group in df.groupby('window_start'):
#         # Basic statistics
#         total_size = group['Size'].sum()
#         packets_count = len(group)
#         avg_size = group['Size'].mean()
        
#         # Direction counts
#         direction_counter = Counter(group['Direction'])
#         direction_0_count = direction_counter.get(0, 0)
#         direction_1_count = direction_counter.get(1, 0)
#         most_common_direction = direction_counter.most_common(1)[0][0] if packets_count > 0 else None
        
#         # Get window timestamps in milliseconds
#         window_start_ms = int(window_start * 1000)
#         window_end_ms = int((window_start + window_size) * 1000)
        
#         # Calculate time from start in seconds
#         time_from_start = window_start - start_time
        
#         stats = {
#             'window_start_ms': window_start_ms,
#             'window_end_ms': window_end_ms,
#             'ms_from_origin': window_start_ms,  # Milliseconds from timestamp origin
#             'time_from_start': time_from_start,  # Time from start in seconds
#             'window_start_time': window_start,
#             'window_end_time': window_start + window_size,
#             'total_size': total_size,
#             'packets_count': packets_count,
#             'packets_direction_0': direction_0_count,  # Count of packets in direction 0
#             'packets_direction_1': direction_1_count,  # Count of packets in direction 1
#             'average_size': avg_size,
#             'most_common_direction': most_common_direction,
#             'direction_0_ratio': direction_0_count / packets_count if packets_count > 0 else 0,
#             'direction_1_ratio': direction_1_count / packets_count if packets_count > 0 else 0,
#             # 'packets_per_window': packets_count,
#             # 'packets_per_second': packets_count * (1000 / window_size_ms)  # Normalized to per-second rate
#         }
        
#         aggregated_stats.append(stats)
    
#     # Create DataFrame from aggregated statistics
#     result_df = pd.DataFrame(aggregated_stats)
    
#     # Sort by time
#     result_df = result_df.sort_values('window_start_ms').reset_index(drop=True)
    
#     # Save to CSV
#     result_df.to_csv(output_file, index=False)
    
#     # Print some summary statistics
#     total_intervals = len(result_df)
#     print(f"\nProcessed {len(df)} packets into {total_intervals} {window_size_ms}ms windows")
#     if total_intervals > 0:
#         print(f"\nTime range: {result_df['window_start_ms'].min()} to {result_df['window_end_ms'].max()} ms")
#         print(f"Total duration: {(result_df['window_end_ms'].max() - result_df['window_start_ms'].min()) / 1000:.3f} seconds")
#         print(f"\nAverage packets per window: {result_df['packets_count'].mean():.3f}")
#         print(f"Max packets in one window: {result_df['packets_count'].max()}")
#         print(f"Min packets in one window: {result_df['packets_count'].min()}")
        
#         # Print example of the windowed data
#         print(f"\nExample of {window_size_ms}ms window data:")
#         columns_to_show = ['window_start_ms', 'ms_from_origin', 'time_from_start', 
#                           'packets_count', 'packets_direction_0', 'packets_direction_1']
#         print(result_df[columns_to_show].head(6))
    
#     return result_df


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Process QUIC traffic data and create an aggregated CSV file with millisecond window statistics.")
#     parser.add_argument("--input", required=True, help="Path to the input CSV file")
#     parser.add_argument("--output", required=True, help="Path where the output CSV will be saved")
#     parser.add_argument("--window", type=int, default=1, help="Window size in milliseconds (default: 1)")
#     args = parser.parse_args()
    
#     # Process the data
#     aggregated_df = process_quic_traffic(args.input, args.output, args.window)
    
#     # Display the first few rows of the processed data
#     print("\nFirst few rows of the processed data:")
#     print(aggregated_df.head())


# import pandas as pd
# import numpy as np
# from collections import Counter
# import argparse


# def process_quic_traffic(input_file, output_file, window_size_ms=1):
#     """
#     Process QUIC traffic data and create an aggregated CSV file with millisecond window statistics.
#     Includes empty rows for time windows with no traffic.
#     """
#     # Read the original CSV file
#     df = pd.read_csv(input_file)
    
#     # Convert timestamps to milliseconds with higher precision
#     df['ms_timestamp'] = (df['Timestamp'] * 1000).astype(int)  # Convert to integer milliseconds
    
#     # Create windows at millisecond precision
#     df['window_start_ms'] = (df['ms_timestamp'] // window_size_ms) * window_size_ms
#     df['window_end_ms'] = df['window_start_ms'] + window_size_ms
    
#     # Convert back to seconds for time calculations
#     df['window_start'] = df['window_start_ms'] / 1000
#     df['window_end'] = df['window_end_ms'] / 1000
    
#     # Print diagnostic information
#     print("Timestamp analysis:")
#     print(f"Original timestamp range: {df['Timestamp'].min()} to {df['Timestamp'].max()}")
#     print(f"Millisecond timestamp range: {df['ms_timestamp'].min()} to {df['ms_timestamp'].max()}")
#     print(f"Window starts range: {df['window_start_ms'].min()} to {df['window_start_ms'].max()}")
#     print(f"Number of unique windows with data: {df['window_start_ms'].nunique()}")
    
#     # Group by window start time
#     aggregated_stats = []
    
#     for window_start_ms, group in df.groupby('window_start_ms'):
#         # Basic statistics
#         total_size = group['Size'].sum()
#         packets_count = len(group)
#         avg_size = group['Size'].mean()
        
#         # Direction counts
#         direction_counter = Counter(group['Direction'])
#         direction_0_count = direction_counter.get(0, 0)
#         direction_1_count = direction_counter.get(1, 0)
#         most_common_direction = direction_counter.most_common(1)[0][0] if packets_count > 0 else -1
        
#         window_end_ms = window_start_ms + window_size_ms
        
#         # Calculate time from start relative to the first window
#         time_from_start = (window_start_ms - df['window_start_ms'].min()) / 1000  # Convert to seconds
        
#         stats = {
#             'window_start_ms': window_start_ms,
#             'window_end_ms': window_end_ms,
#             'ms_from_origin': window_start_ms,
#             'time_from_start': time_from_start,
#             'total_size': total_size,
#             'packets_count': packets_count,
#             'packets_direction_0': direction_0_count,
#             'packets_direction_1': direction_1_count,
#             'average_size': avg_size,
#             'most_common_direction': most_common_direction,
#             'direction_0_ratio': direction_0_count / packets_count if packets_count > 0 else 0,
#             'direction_1_ratio': direction_1_count / packets_count if packets_count > 0 else 0
#         }
        
#         aggregated_stats.append(stats)
    
#     # Create DataFrame from aggregated statistics
#     result_df = pd.DataFrame(aggregated_stats)
    
#     # Create a complete time series with all possible windows
#     min_window = result_df['window_start_ms'].min()
#     max_window = result_df['window_end_ms'].max()
    
#     # Generate all possible windows at millisecond precision
#     all_windows = pd.DataFrame({
#         'window_start_ms': range(min_window, max_window, window_size_ms)
#     })
    
#     # Calculate other time fields for all windows
#     all_windows['window_end_ms'] = all_windows['window_start_ms'] + window_size_ms
#     all_windows['ms_from_origin'] = all_windows['window_start_ms']
#     all_windows['time_from_start'] = (all_windows['window_start_ms'] - min_window) / 1000
    
#     # Merge with actual data
#     complete_df = pd.merge(all_windows, result_df, 
#                           on=['window_start_ms', 'window_end_ms', 'ms_from_origin', 'time_from_start'],
#                           how='left')
    
#     # Fill NaN values
#     numeric_columns = ['total_size', 'packets_count', 'packets_direction_0', 
#                       'packets_direction_1', 'average_size', 'direction_0_ratio', 
#                       'direction_1_ratio']
#     complete_df[numeric_columns] = complete_df[numeric_columns].fillna(0)
#     complete_df['most_common_direction'] = complete_df['most_common_direction'].fillna(-1)
    
#     # Sort by time
#     complete_df = complete_df.sort_values('window_start_ms').reset_index(drop=True)
    
#     # Print distribution of timestamps to help diagnose any patterns
#     print("\nWindow distribution analysis:")
#     print("Sample of window start times (first 10 non-empty windows):")
#     non_empty_windows = complete_df[complete_df['packets_count'] > 0]['window_start_ms'].head(10)
#     print(non_empty_windows.values)
    
#     # Save to CSV
#     complete_df.to_csv(output_file, index=False)
    
#     # Print summary statistics
#     total_intervals = len(complete_df)
#     active_intervals = len(result_df)
#     print(f"\nProcessed {len(df)} packets into {total_intervals} {window_size_ms}ms windows")
#     print(f"Active windows: {active_intervals}")
#     print(f"Empty windows: {total_intervals - active_intervals}")
    
#     if total_intervals > 0:
#         print(f"\nTime range: {complete_df['window_start_ms'].min()} to {complete_df['window_end_ms'].max()} ms")
#         print(f"Total duration: {(complete_df['window_end_ms'].max() - complete_df['window_start_ms'].min()) / 1000:.3f} seconds")
#         print(f"\nAverage packets per active window: {complete_df['packets_count'].mean():.3f}")
#         print(f"Max packets in one window: {complete_df['packets_count'].max()}")
        
#         # Print example of the windowed data
#         print(f"\nExample of {window_size_ms}ms window data (including empty windows):")
#         columns_to_show = ['window_start_ms', 'ms_from_origin', 'time_from_start', 
#                           'packets_count', 'packets_direction_0', 'packets_direction_1']
#         print(complete_df[columns_to_show].head(10))
    
#     return complete_df


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Process QUIC traffic data and create an aggregated CSV file with millisecond window statistics.")
#     parser.add_argument("--input", required=True, help="Path to the input CSV file")
#     parser.add_argument("--output", required=True, help="Path where the output CSV will be saved")
#     parser.add_argument("--window", type=int, default=1, help="Window size in milliseconds (default: 1)")
#     args = parser.parse_args()
    
#     # Process the data
#     aggregated_df = process_quic_traffic(args.input, args.output, args.window)

import pandas as pd
import numpy as np
import argparse
import os

def process_traffic_data(input_file, output_dir):
    # Load the CSV file
    df = pd.read_csv(input_file)
    
    # Convert timestamp to milliseconds
    df['Timestamp_ms'] = (df['Timestamp'] * 1000).astype(int)
    
    # Get the starting time for relative calculations
    start_time = df['Timestamp_ms'].min()
    
    # Create a complete range of timestamps in milliseconds
    all_timestamps = pd.DataFrame({'Timestamp_ms': np.arange(start_time, df['Timestamp_ms'].max() + 1)})
    
    # Create a new dataframe grouped by millisecond bins
    grouped = df.groupby('Timestamp_ms').agg(
        total_packets=('Size', 'count'),
        most_common_direction=('Direction', lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan),
        average_size=('Size', 'mean'),
        packets_per_direction=('Direction', lambda x: x.value_counts().to_dict())
    ).reset_index()
    
    # Calculate time from start in milliseconds
    grouped['time_from_start'] = grouped['Timestamp_ms'] - start_time
    
    # Extract packet counts per direction and compute direction ratio
    def extract_direction_counts(row):
        packets_dict = row['packets_per_direction']
        dir_0 = packets_dict.get(0, 0)
        dir_1 = packets_dict.get(1, 0)
        total = dir_0 + dir_1
        direction_ratio = dir_0 / total if total > 0 else np.nan
        return pd.Series([dir_0, dir_1, direction_ratio])

    grouped[['packets_dir_0', 'packets_dir_1', 'direction_ratio']] = grouped.apply(extract_direction_counts, axis=1)
    
    # Drop the dictionary column
    grouped.drop(columns=['packets_per_direction'], inplace=True)
    
    # Merge with all timestamps to ensure continuity
    final_df = all_timestamps.merge(grouped, on='Timestamp_ms', how='left')
    
    # Save the processed data to CSV
    output_file = os.path.join(output_dir, "processed_traffic_data.csv")
    final_df.to_csv(output_file, index=False)
    print(f"Processed data saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process encrypted traffic data and generate features.")
    parser.add_argument("--input_file", type=str, required=True, help="Path to the input CSV file")
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save the processed CSV file")
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    process_traffic_data(args.input_file, args.output_dir)

