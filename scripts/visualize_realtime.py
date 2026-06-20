#!/usr/bin/env python3
"""
REAL-TIME VISUALIZATION
Reads the live streaming predictions from MongoDB (via Docker) and generates charts.
"""

import os
import subprocess
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUTPUT_DIR = "docs/assets"
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    # Run query inside spark-master to bypass any Windows port mapping issues
    cmd = [
        "docker", "exec", "spark-master", "python3", "-c",
        "from pymongo import MongoClient; import json; "
        "db = MongoClient('mongodb://mongodb:27017/').sentiment_db; "
        "print(json.dumps(list(db.predictions.aggregate([{'$group':{'_id':'$sentiment','count':{'$sum':1}}}]))))"
    ]
    
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"Error querying Docker: {proc.stderr}")
        exit(1)
        
    results = json.loads(proc.stdout.strip())
    
    pos_count = 0
    neg_count = 0
    neu_count = 0
    
    for row in results:
        sentiment = row["_id"]
        count = row["count"]
        if sentiment == "Positive":
            pos_count = count
        elif sentiment == "Negative":
            neg_count = count
        elif sentiment == "Neutral":
            neu_count = count
            
    print(f"Live Data Summary:")
    print(f" - Positive: {pos_count}")
    print(f" - Negative: {neg_count}")
    print(f" - Neutral: {neu_count}")
    
    total = pos_count + neg_count + neu_count
    if total == 0:
        print("No live data found in MongoDB. Please run the Kafka Producer and Spark Streaming first.")
        exit(0)

    # Plot Pie Chart
    labels = []
    sizes = []
    colors = []
    if neg_count > 0:
        labels.append("Negative")
        sizes.append(neg_count)
        colors.append("#e74c3c")
    if neu_count > 0:
        labels.append("Neutral")
        sizes.append(neu_count)
        colors.append("#95a5a6")
    if pos_count > 0:
        labels.append("Positive")
        sizes.append(pos_count)
        colors.append("#2ecc71")

    fig, ax = plt.subplots(figsize=(8, 6))
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, colors=colors,
        autopct="%1.1f%%", startangle=90,
        explode=[0.02] * len(sizes),
        textprops={"fontsize": 12}
    )
    for t in autotexts:
        t.set_fontweight("bold")
    ax.set_title(f"Real-time Sentiment Distribution\n(Total {total} Live Tweets)", fontsize=14, fontweight="bold")
    plt.tight_layout()
    path = f"{OUTPUT_DIR}/realtime_distribution.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    
    print(f"Saved realtime chart to {path}")

except Exception as e:
    print(f"Error generating realtime chart: {e}")
