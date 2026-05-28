import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import os
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "mads_socialist_16"
EXCLUDE_FIELDS = ['_id', 'hostname', 'agent_id', 'id', 'agent_type', 'type', 'message.hostname', 'message.agent_id', 'fmu_input']
GAP_THRESHOLD = 10.0 
SOURCES = {
    'source_solar_1': '#f1c40f', 
    'source_hydro_1': '#3498db', 
    'source_wind_1':  '#95a5a6'   
}
LOADS = ['load_1', 'load_2', 'load_3']

def apply_oscillation_filter(series, window=21):
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
            df = df.sort_values(time_col)
            if start_time: df = df[df[time_col] >= start_time]
            if end_time: df = df[df[time_col] <= end_time]
            
            if topic == 'source_hydro_1':
                prop_col = 'message.state.proposed_power'
                if prop_col in df.columns:
                    df[prop_col] = df[prop_col].rolling(window=5, center=True, min_periods=1).mean()
            
            comparison_data[topic] = df

    load_data = {}
    for topic in LOADS:
        data = list(db[topic].find())
        if not data: continue
        df = pd.json_normalize(data)
        time_col = 'message.timecode'
        req_col = 'message.request'
        if time_col in df.columns and req_col in df.columns:
            df[time_col] = pd.to_numeric(df[time_col])
            df[time_col] = (df[time_col] / 60).round() * 60
            df = df.sort_values(time_col)
            if start_time: df = df[df[time_col] >= start_time]
            if end_time: df = df[df[time_col] <= end_time]
            load_data[topic] = df[[time_col, req_col]].dropna()

    if comparison_data:
        metrics = {
            'p_max': 'message.state.p_max',
            'covariance': 'message.state.covariance',
            'proposed_power': 'message.state.proposed_power'
        }
        
        for metric_name, db_field in metrics.items():
            if metric_name == 'proposed_power': continue 
            
            plt.figure(figsize=(12, 6))
            found_metric = False
            
            if metric_name == 'covariance':
                all_t = pd.concat([df['message.timecode'] for df in comparison_data.values()]).unique()
                combined_t = pd.DataFrame({'message.timecode': sorted(all_t)})
                num_sum = np.zeros(len(combined_t))
                den_sum = np.zeros(len(combined_t))

            for topic, df in comparison_data.items():
                if db_field in df.columns:
                    clean_df = df.dropna(subset=[db_field])
                    plt.plot(clean_df['message.timecode'], clean_df[db_field], label=topic, color=SOURCES[topic], linewidth=2)
                    found_metric = True
                    
                    if metric_name == 'covariance' and 'message.state.p_max' in df.columns:
                        temp_df = df[['message.timecode', db_field, 'message.state.p_max']].dropna()
                        interp_df = pd.merge_asof(combined_t, temp_df, on='message.timecode')
                        
                        cov_i = interp_df[db_field].replace(0, np.nan)
                        pmax_i = interp_df['message.state.p_max']
                        
                        num_sum += ((pmax_i ** 2) / cov_i).fillna(0).values
                        den_sum += (pmax_i / cov_i).fillna(0).values

            if metric_name == 'covariance' and found_metric:
                sigma_tot = np.where(den_sum > 0, num_sum / (den_sum ** 2), np.nan)
                plt.plot(combined_t['message.timecode'], sigma_tot, label='Total Network Covariance (WLS Capacity-Weighted)', color='red', linewidth=3, linestyle='--')
                plt.yscale('log')

            if found_metric:
                plt.title(f"Comparison: {metric_name}")
                plt.xlabel("Timecode [s]")
                plt.ylabel("Value")
                plt.legend()
                plt.grid(True, linestyle='--', alpha=0.5)
                plt.savefig(f"plots/comparison_{metric_name}.png")
            plt.close()
        
        plt.figure(figsize=(12, 6))
        found_combined = False

        for topic, df in comparison_data.items():
            if 'message.state.p_max' in df.columns:
                clean_df_pmax = df.dropna(subset=['message.state.p_max'])
                if not clean_df_pmax.empty:
                    plt.plot(clean_df_pmax['message.timecode'], clean_df_pmax['message.state.p_max'], 
                             label=f"{topic} (p_max)", color=SOURCES[topic], linewidth=2, linestyle='-')
                    found_combined = True
            
            if 'message.state.proposed_power' in df.columns:
                clean_df_prop = df.dropna(subset=['message.state.proposed_power'])
                if not clean_df_prop.empty:
                    plt.plot(clean_df_prop['message.timecode'], clean_df_prop['message.state.proposed_power'], 
                             label=f"{topic} (proposed)", color=SOURCES[topic], linewidth=1.5, linestyle='-.')
                    found_combined = True

        all_times = []
        for df in comparison_data.values():
            all_times.extend(df['message.timecode'].tolist())
        for df in load_data.values():
            all_times.extend(df['message.timecode'].tolist())
        
        if all_times:
            unified_t = pd.DataFrame({'message.timecode': sorted(list(set(all_times)))})
            
            total_proposed = pd.Series(np.zeros(len(unified_t)), index=unified_t.index)
            for topic, df in comparison_data.items():
                if 'message.state.proposed_power' in df.columns:
                    clean_df_prop = df[['message.timecode', 'message.state.proposed_power']].dropna()
                    if not clean_df_prop.empty:
                        merged_prop = pd.merge_asof(unified_t, clean_df_prop, on='message.timecode', direction='backward')
                        req = merged_prop['message.state.proposed_power'].bfill().fillna(0)
                        total_proposed += req
            
            total_proposed = total_proposed.rolling(window=1001, center=True, min_periods=1).median()
            
            plt.plot(unified_t['message.timecode'], total_proposed, 
                     label="Total Output Power (Dispatched)", color='#8e44ad', linewidth=2, linestyle='-.')
            found_combined = True

            if load_data:
                total_demand = pd.Series(np.zeros(len(unified_t)), index=unified_t.index)
                for load_topic, df in load_data.items():
                    merged = pd.merge_asof(unified_t, df, on='message.timecode', direction='backward')
                    req = merged['message.request'].bfill().fillna(0) 
                    total_demand += req
                
                total_demand = total_demand.rolling(window=301, center=True, min_periods=1).median()
                
                plt.plot(unified_t['message.timecode'], total_demand, 
                         label="Total P_Demand (Sum of Loads)", color='gray', linewidth=2, linestyle='--', alpha=0.5, drawstyle='steps-post')
                found_combined = True

        if found_combined:
            plt.title("Comparison: P_max vs Total Output Power vs Total Demand")
            plt.xlabel("Timecode [s]")
            plt.ylabel("Power [W]")
            plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.tight_layout()
            plt.savefig("plots/comparison_pmax_vs_proposed.png")
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