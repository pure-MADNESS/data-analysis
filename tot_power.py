import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import os
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "mads_socialist_4"
FIELD_LOAD = 'message.request'
FIELD_SOURCE = 'message.state.proposed_power'
TIME_STAMP_COL = 'message.timestamp'

def fetch_and_plot(start_time=None, end_time=None):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    all_data_list = []

    for topic in db.list_collection_names():
        data = list(db[topic].find())
        if not data: continue
        
        df = pd.json_normalize(data)
        if TIME_STAMP_COL in df.columns:
            df[TIME_STAMP_COL] = pd.to_datetime(df[TIME_STAMP_COL])
            
            time_col = 'message.timecode'
            if time_col in df.columns:
                df[time_col] = pd.to_numeric(df[time_col])
                if start_time: df = df[df[time_col] >= start_time]
                if end_time: df = df[df[time_col] <= end_time]
            
            if df.empty: continue
            
            cols = [TIME_STAMP_COL]
            if FIELD_LOAD in df.columns: cols.append(FIELD_LOAD)
            if FIELD_SOURCE in df.columns: cols.append(FIELD_SOURCE)
            all_data_list.append(df[cols].copy())

    if not all_data_list:
        print("Nessun dato trovato.")
        return

    global_df = pd.concat(all_data_list, sort=False).sort_values(TIME_STAMP_COL)
    global_df.set_index(TIME_STAMP_COL, inplace=True)

    # 1. Resampling stretto per calcolare il bilancio istantaneo
    resampled = global_df.resample('10s').mean().fillna(0)

    # 2. Calcolo deficit istantaneo: se la richiesta è > produzione, prendo dalla rete
    resampled['grid_diff'] = (resampled[FIELD_LOAD] - resampled[FIELD_SOURCE]).clip(lower=0)

    # 3. Report finale: usiamo la MEDIA per tutto così i numeri sono confrontabili
    # Se vuoi l'energia totale (kWh), moltiplicheremo dopo.
    final_report = resampled.resample('10min').mean() 

    print("\n" + "="*95)
    print(f"{'TIME WINDOW':<15} | {'AVG LOAD':<15} | {'AVG GEN':<15} | {'AVG PUB GRID':<15} | {'TOT Wh'}")
    print("-" * 95)
    
    for index, row in final_report.iterrows():
        if row[FIELD_LOAD] > 0 or row[FIELD_SOURCE] > 0:
            # Calcolo energia: Potenza media * tempo (10 min = 1/6 di ora)
            kwh = row['grid_diff'] * (10/60) 
            
            print(f"{index.strftime('%H:%M'):<15} | "
                  f"{row[FIELD_LOAD]:>15.2f} | "
                  f"{row[FIELD_SOURCE]:>15.2f} | "
                  f"{row['grid_diff']:>15.2f} | " # Ora questo è Load - Gen
                  f"{kwh:>8.2f}")

    print("="*95)

    total_kwh = (final_report['grid_diff'] * (10/60)).sum()

    print("\n" + "="*95)
    print(f"{total_kwh:.2f} Wh prelevati dalla rete pubblica")
    print("="*95 + "\n")

if __name__ == "__main__":
    fetch_and_plot()