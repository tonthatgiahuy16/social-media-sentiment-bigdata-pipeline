from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
import subprocess
import json
import os

app = FastAPI()

# Mount static files (HTML, CSS, JS)
app.mount("/static", StaticFiles(directory="dashboard"), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_index():
    with open("dashboard/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/metrics")
async def get_metrics():
    """Fetch metrics from HDFS metrics.json"""
    try:
        # Command to cat the file from HDFS
        cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", "/sentiment/models/metrics.json"]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            return json.loads(proc.stdout)
        else:
            return {"error": "Metrics file not found on HDFS"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/realtime")
async def get_realtime_metrics():
    """Fetch live prediction counts from MongoDB"""
    try:
        cmd = [
            "docker", "exec", "spark-master", "python3", "-c",
            "from pymongo import MongoClient; import json; "
            "db = MongoClient('mongodb://mongodb:27017/').sentiment_db; "
            "print(json.dumps(list(db.predictions.aggregate([{'$group':{'_id':'$sentiment','count':{'$sum':1}}}]))))"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            return json.loads(proc.stdout.strip())
        else:
            return []
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/predict")
async def predict(data: dict):
    """Placeholder for Spark Model Prediction"""
    text = data.get("text", "")
    # In a real scenario, you would call a Spark job or use a pre-loaded model here.
    # For the demo, we use a simple logic or call a micro-spark-submit.
    
    # Simulate a result
    return {
        "text": text,
        "sentiment": "POSITIVE" if "good" in text.lower() or "love" in text.lower() else "NEGATIVE",
        "confidence": 0.89
    }

if __name__ == "__main__":
    import uvicorn
    print("\n" + "="*50)
    print("  BIG DATA DASHBOARD RUNNING AT http://localhost:8001")
    print("="*50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8001)
