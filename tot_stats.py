import pandas as pd
from pymongo import MongoClient
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "mads_socialist_5"
FIELD_LOAD = 'message.request'
FIELD_SOURCE = 'message.state.proposed_power'
TIME_STAMP_COL = 'message.timestamp'

def calculate_wh(df, column_name):
    """Calcola l'energia totale (Wh) usando l'integrazione trapezoidale"""
    if df.empty or column_name not in df.columns:
        return 0.0
    
    df = df.sort_values(TIME_STAMP_COL)
    
    dt_seconds = df[TIME_STAMP_COL].diff().dt.total_seconds().fillna(0)
    
    avg_power = (df[column_name] + df[column_name].shift(1)) / 2
    avg_power = avg_power.fillna(0)
    
    wh_intervals = (avg_power * dt_seconds) / 3600.0
    
    return wh_intervals.sum()

def fetch_and_calculate_energy_total():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    total_consumed_wh = 0.0
    total_generated_wh = 0.0
    
    all_timestamps = []

    for topic in db.list_collection_names():
        data = list(db[topic].find())
        if not data: continue
        
        df = pd.json_normalize(data)
        if TIME_STAMP_COL not in df.columns: continue
        
        df[TIME_STAMP_COL] = pd.to_datetime(df[TIME_STAMP_COL])
        all_timestamps.extend(df[TIME_STAMP_COL].tolist())
        
        total_consumed_wh += calculate_wh(df, FIELD_LOAD)
        total_generated_wh += calculate_wh(df, FIELD_SOURCE)

    if not all_timestamps:
        print("Nessun dato trovato nel database.")
        return

    start_time = min(all_timestamps)
    end_time = max(all_timestamps)
    duration = end_time - start_time

    grid_balance = total_consumed_wh - total_generated_wh

    print("\n" + "═"*60)
    print(f" REPORT ENERGETICO MADNESS")
    print("─"*60)
    print(f"{'Inizio Test:':<20} {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'Fine Test:':<20} {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'Durata Totale:':<20} {duration}")
    print("─"*60)
    print(f"{'Energia Consumata:':<25} {total_consumed_wh / 1000:>10.3f} kWh")
    print(f"{'Energia Generata:':<25} {total_generated_wh / 1000:>10.3f} kWh")
    print("─"*60)
    print(f"{'PRELEVATO DA RETE:':<25} {grid_balance / 1000:>10.3f} kWh")
    print("═"*60 + "\n")

if __name__ == "__main__":
    fetch_and_calculate_energy_total()