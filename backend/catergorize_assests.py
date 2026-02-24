import os
import logging
from read_data import matched

output_path = "C:/Users/murta/Desktop/EasyTrade/asset_data"

logger = logging.getLogger("catergorize_assests")
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler("catergorize_assests.log", mode="w"))

def group_assets(df):
    os.makedirs(output_path, exist_ok=True)
    for asset, asset_df in df.groupby("asset"):
        asset_df = asset_df.reset_index(drop=True)
        asset_df.to_csv(os.path.join(output_path, f"{asset}.csv"), index=False)
        

group_assets(matched)
logger.info(f"Wrote {len(matched)} rows to {output_path}")# test the function