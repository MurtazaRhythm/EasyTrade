import pandas as pd
import numpy as np
import os
import logging

test_datasets_path = "C:/Users/murta/Desktop/EasyTrade/test_datasets"

logger = logging.getLogger("read_data")
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler("easytrade.log", mode="w"))

def read_data(file_path):
    df = pd.read_csv(file_path)
    logger.info(f"Read {len(df)} rows from {file_path}")
    return df

def read_all_data():
    frames = []
    for dataset in os.listdir(test_datasets_path):
        if dataset.endswith(".csv"):
            df = read_data(os.path.join(test_datasets_path, dataset))
            frames.append(df)
            logger.info(f"Read {len(df)} rows from {dataset}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    

def match_data():
    df = read_all_data().copy()
    df = df.sort_values("timestamp").reset_index(drop=True)

    buy_queues = {}
    matched_rows = []

    for _, row in df.iterrows():
        asset = row["asset"]
        if row["side"] == "BUY":
            buy_queues.setdefault(asset, []).append(row)
        elif row["side"] == "SELL" and buy_queues.get(asset):
            buy_row = buy_queues[asset].pop(0)
            matched_rows.append({
                "asset":           asset,
                "buy_timestamp":   buy_row["timestamp"],
                "sell_timestamp":  row["timestamp"],
                "quantity":        row["quantity"],
                "entry_price":     buy_row["entry_price"],
                "exit_price":      row["exit_price"],
                "profit_loss":     row["profit_loss"],
            })
        

    return pd.DataFrame(matched_rows)

matched = match_data()
logger.info(matched.head(10))

