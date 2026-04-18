import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import os

# config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "mads_socialist_2"
EXCLUDE_FIELDS = ['_id', 'hostname', 'agent_id', 'id', 'agent_type', 'type', 'message.hostname', 'message.agent_id']

def fetch_and_plot():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    os.makedirs('plots', exist_ok=True)

    for topic in db.list_collection_names():
        print(f"Processing topic: {topic}...")
        
        data = list(db[topic].find())
        if not data:
            continue
            
        df = pd.json_normalize(data)
        
        time_col = 'message.timecode'
        
        if time_col in df.columns:
            df[time_col] = pd.to_numeric(df[time_col])
            df = df.sort_values(time_col)
        else:
            print(f"Skip {topic}: field '{time_col}' not found")
            continue

        numeric_cols = df.select_dtypes(include=['number']).columns
        cols_to_plot = [c for c in numeric_cols if c != time_col and not any(ex in c for ex in EXCLUDE_FIELDS)]
        
        cols_to_plot = [c for c in cols_to_plot if 'hourly' not in c]

        if not cols_to_plot:
            print(f"No plots for {topic}.")
            continue

        fig, axes = plt.subplots(len(cols_to_plot), 1, figsize=(12, 4 * len(cols_to_plot)), sharex=True)
        
        if len(cols_to_plot) == 1:
            axes = [axes]

        for i, col in enumerate(cols_to_plot):
            axes[i].plot(df[time_col], df[col], label=col, color='tab:blue', linewidth=1.5)
            axes[i].set_title(f"Topic: {topic} | Field: {col}")
            axes[i].set_ylabel("Value")
            axes[i].grid(True, linestyle='--', alpha=0.6)
            axes[i].legend(loc='upper right')

        plt.xlabel("Timecode [s]")
        plt.tight_layout()
        
        file_name = f"plots/{topic.replace('/', '_')}.png"
        plt.savefig(file_name)
        plt.close()
        print(f"Pic saved: {file_name}")

    print("\nDone.")

if __name__ == "__main__":
    fetch_and_plot()