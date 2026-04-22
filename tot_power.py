import pandas as pd
from pymongo import MongoClient
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "mads_socialist_4"
FIELD_LOAD = 'message.request'
FIELD_SOURCE = 'message.state.proposed_power'
TIME_STAMP_COL = 'message.timestamp'
OSCILLATION_THRESHOLD = 200.0 # W

def fetch_and_calculate_energy():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    all_data_list = []

    for topic in db.list_collection_names():
        data = list(db[topic].find())
        if not data: continue
        
        df = pd.json_normalize(data)
        if TIME_STAMP_COL in df.columns:
            df[TIME_STAMP_COL] = pd.to_datetime(df[TIME_STAMP_COL])
            
            cols = [TIME_STAMP_COL]
            target_col = None
            if FIELD_LOAD in df.columns: target_col = FIELD_LOAD
            elif FIELD_SOURCE in df.columns: target_col = FIELD_SOURCE
            
            if target_col:
                cols = [TIME_STAMP_COL, target_col]
                temp_df = df[cols].copy()
                
                temp_df[TIME_STAMP_COL] = pd.to_datetime(temp_df[TIME_STAMP_COL])
                temp_df = temp_df.sort_values(TIME_STAMP_COL)
                
                temp_df = temp_df.set_index(TIME_STAMP_COL)
                
                rolling_max = temp_df[target_col].rolling('30s').max()
                rolling_std = temp_df[target_col].rolling('30s').std()
                
                temp_df[target_col] = np.where(rolling_std > OSCILLATION_THRESHOLD, 
                                             rolling_max, 
                                             temp_df[target_col])
                
                temp_df = temp_df.reset_index()
                
                all_data_list.append(temp_df)

    if not all_data_list:
        print("Nessun dato trovato.")
        return

    global_df = pd.concat(all_data_list, sort=False).sort_values(TIME_STAMP_COL)
    global_df.set_index(TIME_STAMP_COL, inplace=True)

    hourly_data = global_df.resample('1h').mean().fillna(0)

    print("\n" + "="*95)
    print(f"{'TIME HOUR':<15} | {'AVG LOAD [W]':<15} | {'AVG GEN [W]':<15} | {'FROM GRID [Wh]'}")
    print("-" * 95)
    
    total_grid_wh = 0.0

    for index, row in hourly_data.iterrows():
        load = row[FIELD_LOAD]
        gen = row[FIELD_SOURCE]
        
        grid_power_deficit = max(0, load - gen)
        
        grid_wh_hour = grid_power_deficit * 1.0
        total_grid_wh += grid_wh_hour
        
        if load > 0 or gen > 0:
            print(f"{index.strftime('%H:00'):<15} | "
                  f"{load:>15.2f} | "
                  f"{gen:>15.2f} | "
                  f"{grid_wh_hour:>15.2f}")

    print("="*95)
    print(f"TOTALE PRELEVATO DALLA RETE: {total_grid_wh / 1000:.3f} kWh")
    print("="*95 + "\n")

if __name__ == "__main__":
    fetch_and_calculate_energy()