import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import os
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "mads_socialist_7"
EXCLUDE_FIELDS = ['_id', 'hostname', 'agent_id', 'id', 'agent_type', 'type', 'message.hostname', 'message.agent_id', 'fmu_input']
GAP_THRESHOLD = 10.0 
SOURCES = {
    'source_solar_1': '#f1c40f', 
    'source_hydro_1': '#3498db', 
    'source_wind_1':  '#95a5a6'   
}

def apply_oscillation_filter(series, window=20):
    if len(series) < window:
        return series
    return series.rolling(window=window, center=True, min_periods=1).min()

def fetch_and_plot(start_time=None, end_time=None):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    os.makedirs('plots', exist_ok=True)

    comparison_data = {}
    for topic in SOURCES.keys():
        data = list(db[topic].find())
        if not data: continue
        df = pd.json_normalize(data)
        time_col = 'message.timecode'
        if time_col in df.columns:
            df[time_col] = pd.to_numeric(df[time_col])
            if start_time: df = df[df[time_col] >= start_time]
            if end_time: df = df[df[time_col] <= end_time]
            df = df.sort_values(time_col)
            
            target_cols = ['message.state.p_max', 'message.state.covariance', 'message.state.proposed_power']
            for col in target_cols:
                if col in df.columns:
                    df[col] = apply_oscillation_filter(df[col])
            
            comparison_data[topic] = df

    if comparison_data:
        metrics = {
            'p_max': 'message.state.p_max',
            'covariance': 'message.state.covariance',
            'proposed_power': 'message.state.proposed_power'
        }
        for metric_name, db_field in metrics.items():
            plt.figure(figsize=(12, 6))
            found_metric = False
            for topic, df in comparison_data.items():
                if db_field in df.columns:
                    clean_df = df.dropna(subset=[db_field])
                    plt.plot(clean_df[time_col], clean_df[db_field], label=topic, color=SOURCES[topic], linewidth=2)
                    found_metric = True
            if found_metric:
                plt.title(f"Comparison: {metric_name} (Filtered)")
                plt.xlabel("Timecode [s]")
                plt.ylabel("Value")
                plt.legend()
                plt.grid(True, linestyle='--', alpha=0.5)
                plt.savefig(f"plots/comparison_{metric_name}.png")
            plt.close()

    for topic in db.list_collection_names():
        data = list(db[topic].find())
        if not data: continue
        df = pd.json_normalize(data)
        time_col = 'message.timecode'
        if time_col not in df.columns: continue
        df[time_col] = pd.to_numeric(df[time_col])
        df = df.sort_values(time_col)
        if start_time: df = df[df[time_col] >= start_time]
        if end_time: df = df[df[time_col] <= end_time]
        if df.empty: continue

        diff = df[time_col].diff()
        mask = diff > GAP_THRESHOLD
        if mask.any():
            new_rows = df[mask].copy()
            for col in df.columns:
                new_rows[col] = (new_rows[col] - 0.001) if col == time_col else np.nan
            df = pd.concat([df, new_rows]).sort_values(time_col)

        numeric_cols = df.select_dtypes(include=['number']).columns
        cols_to_plot = [c for c in numeric_cols if c != time_col and not any(ex in c for ex in EXCLUDE_FIELDS)]
        cols_to_plot = [c for c in cols_to_plot if 'hourly' not in c]
        
        for col in cols_to_plot:
            df[col] = apply_oscillation_filter(df[col])

        if not cols_to_plot: continue
        fig, axes = plt.subplots(len(cols_to_plot), 1, figsize=(12, 4 * len(cols_to_plot)), sharex=True)
        if len(cols_to_plot) == 1: axes = [axes]
        for i, col in enumerate(cols_to_plot):
            axes[i].plot(df[time_col], df[col], label=col, color='#2e5a27', linewidth=1.5)
            axes[i].set_title(f"Topic: {topic} | Field: {col}")
            axes[i].grid(True, linestyle='--', alpha=0.6)
        plt.xlabel("Timecode [s]")
        plt.tight_layout()
        plt.savefig(f"plots/{topic.replace('/', '_')}.png")
        plt.close()

if __name__ == "__main__":
    fetch_and_plot()