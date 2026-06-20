from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os
from typing import List, Optional
from pydantic import BaseModel

app = FastAPI(
    title="Sentiment Analysis API",
    description="REST API for Sentiment Analysis Pipeline — Layer 3 Model Deployment",
    version="1.0"
)

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "sentiment_db")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


class ModelMetric(BaseModel):
    model_name: str
    accuracy: float
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: float
    auc_roc: Optional[float] = None
    train_time_sec: Optional[float] = None
    is_best: Optional[bool] = None
    timestamp: str


class SentimentTrend(BaseModel):
    label: str
    count: int
    percentage: float


@app.get("/")
async def root():
    return {
        "message": "Sentiment Analysis API is running",
        "status": "healthy",
        "models": ["Random Forest", "Linear SVM", "MLP (LSTM proxy)"],
        "pipeline": "4-Layer Architecture"
    }


@app.get("/metrics", response_model=List[ModelMetric])
async def get_metrics():
    """Retrieve ML model evaluation metrics (Random Forest, SVM, MLP)"""
    metrics = list(db.model_metrics.find({}, {"_id": 0}))
    return metrics


@app.get("/model-metadata")
async def get_model_metadata():
    """Get best model metadata (Layer 3 deployment info)"""
    metadata = db.model_metadata.find_one({}, {"_id": 0})
    if metadata:
        return metadata
    raise HTTPException(status_code=404, detail="No model metadata found. Run ml_training.py first.")


@app.get("/trends", response_model=List[SentimentTrend])
async def get_trends():
    """Retrieve latest sentiment trends (from Spark Streaming)"""
    trends = list(db.sentiment_trends.find({}, {"_id": 0}))
    return trends


@app.get("/recent-predictions")
async def get_recent_predictions(limit: int = 20):
    """Get batch predictions from the best model"""
    predictions = list(
        db.predictions.find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(limit)
    )
    return predictions


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
