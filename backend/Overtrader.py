import pandas as pd
import numpy as np
import os

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)

input_folder = os.path.join(root_dir, "asset_data")
output_folder = os.path.join(root_dir, "analyzed_data")

def detect_overtrading_patterns(df):
    """
    Detects behavioral overtrading patterns and adds boolean flags.
    Fixes the '1H' window error by ensuring a sorted, unique-friendly DatetimeIndex.
    """
    # 1. Convert timestamps and drop invalid rows
    df['buy_timestamp'] = pd.to_datetime(df['buy_timestamp'], errors='coerce')
    df['sell_timestamp'] = pd.to_datetime(df['sell_timestamp'], errors='coerce')
    df = df.dropna(subset=['buy_timestamp', 'sell_timestamp'])
    
    # 2. Sort by buy_timestamp - CRITICAL for time-based rolling
    df = df.sort_values('buy_timestamp').reset_index(drop=True)

    # --- PATTERN 1: Time-Based Clustering (Fixed) ---
    # We create a temporary series with a DatetimeIndex for the rolling calculation
    # We use '1h' (lowercase) which is often more compatible across Pandas versions
    temp_series = df.set_index('buy_timestamp')['asset']
    
    # ensure index is sorted (even if the DF was, setting the index can sometimes jitter it)
    temp_series = temp_series.sort_index()
    
    # We calculate the rolling count. 
    # To avoid alignment errors, we map the results back to the original df
    counts = temp_series.rolling('1h').count()
    
    # Since map() might struggle with duplicate timestamps, we use transform or join logic
    # This is the safest way to ensure the count matches the original row index
    df['is_clustering'] = df.index.map(counts.reset_index(drop=True)) > 5

    # --- PATTERN 2: Frequent Switching ---
    hold_duration_mins = (df['sell_timestamp'] - df['buy_timestamp']).dt.total_seconds() / 60
    df['is_freq_switching'] = hold_duration_mins < 5

    # --- PATTERN 3: Revenge Trading ---
    # Shift based on previous trade's EXIT time (sell) vs current trade's ENTRY time (buy)
    loss_threshold = df['profit_loss'].quantile(0.25)
    df['prev_sell_time'] = df['sell_timestamp'].shift(1)
    df['prev_pl'] = df['profit_loss'].shift(1)
    
    time_gap = (df['buy_timestamp'] - df['prev_sell_time']).dt.total_seconds() / 60
    df['is_revenge_trading'] = (time_gap < 30) & (df['prev_pl'] < loss_threshold)

    # --- PATTERN 4: Excessive Quantity ---
    # Compare current quantity against the average of the PREVIOUS 10 trades
    df['rolling_avg_qty'] = df['quantity'].shift(1).rolling(window=10, min_periods=1).mean()
    df['is_excessive_qty'] = df['quantity'] > (df['rolling_avg_qty'] * 2)

    # Clean up temporary columns
    return df.drop(columns=['prev_sell_time', 'prev_pl', 'rolling_avg_qty'], errors='ignore')

def process_assets():
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created: {output_folder}")

    if not os.path.exists(input_folder):
        print(f"Error: Could not find 'asset_data' folder at {input_folder}")
        return

    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    analyzed_df = detect_overtrading_patterns(df)
                    save_path = os.path.join(output_folder, f"analyzed_{filename}")
                    analyzed_df.to_csv(save_path, index=False)
                    print(f"Saved to: {os.path.abspath(save_path)}")
                    print(f"Successfully analyzed: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    process_assets()