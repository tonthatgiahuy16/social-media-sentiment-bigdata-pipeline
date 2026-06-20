#!/usr/bin/env python3
"""
STAGE 3: ML Training & Evaluation - PySpark MLlib
Sentiment Analysis Pipeline (Layer 3: Machine Learning Layer)

Architecture (from diagram):
  ┌─────────────────┐   ┌──────────────────┐   ┌──────────────────┐
  │  Model Training  │→ │ Model Evaluation  │→ │ Model Deployment  │
  │  - Random Forest │   │ - Accuracy       │   │ - Batch Predict  │
  │  - SVM           │   │ - Precision      │   │ - Save to MongoDB│
  │  - LSTM (MLP)    │   │ - Recall         │   │                  │
  │  + Hyperparam    │   │ - F1-score       │   │                  │
  │    Tuning (CV)   │   │ - AUC-ROC        │   │                  │
  └─────────────────┘   └──────────────────┘   └──────────────────┘

Models:
  1. Random Forest Classifier
  2. Linear SVM (Support Vector Machine)
  3. Multilayer Perceptron (LSTM proxy – PySpark MLlib equivalent)

MLOps: MLflow Tracking, Model Registry, Versioning
Output: Predictions / Scores / Metadata → MongoDB (Real-time)
"""

import time
import sys
import json
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import (
    RandomForestClassifier,
    LinearSVC,
    MultilayerPerceptronClassifier
)
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
HDFS_BASE = "hdfs://namenode:9000/sentiment"
TRAIN_PATH = f"{HDFS_BASE}/processed/features_train"
TEST_PATH = f"{HDFS_BASE}/processed/features_test"
MODEL_SAVE_PATH = f"{HDFS_BASE}/models"
METRICS_HDFS_PATH = f"{HDFS_BASE}/models/metrics.json"
PREDICTIONS_HDFS_PATH = f"{HDFS_BASE}/models/predictions"

MONGODB_URI = "mongodb://mongodb:27017/"
MONGODB_DB = "sentiment_db"

FEATURE_COL = "tfidf_features"
LABEL_COL = "label"
NUM_FOLDS = 2  # Giảm CrossValidation xuống 2 fold cho máy yếu


# ─────────────────────────────────────────────────────────────
# CREATE SPARK SESSION
# ─────────────────────────────────────────────────────────────
def create_spark_session(app_name="SentimentML"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.driver.memory", "800m") \
        .getOrCreate()


# ─────────────────────────────────────────────────────────────
# MODEL EVALUATION
# ─────────────────────────────────────────────────────────────
def evaluate_model(predictions, model_name):
    """
    Evaluate a trained model using standard classification metrics.
    Returns dict with: accuracy, precision, recall, f1, auc
    """
    results = {}

    metrics = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="accuracy"
        ),
        "precision": MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="weightedPrecision"
        ),
        "recall": MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="weightedRecall"
        ),
        "f1": MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="f1"
        ),
        "auc": BinaryClassificationEvaluator(
            labelCol=LABEL_COL, rawPredictionCol="rawPrediction"
        ),
    }

    print(f"\n{'='*60}")
    print(f"  MODEL EVALUATION: {model_name}")
    print(f"{'='*60}")

    for name, evaluator in metrics.items():
        try:
            value = evaluator.evaluate(predictions)
            results[name] = round(value, 4)
            bar = "█" * int(value * 30) + "░" * (30 - int(value * 30))
            print(f"  {name.upper():12s}: {value:.4f}  {bar}")
        except Exception as e:
            print(f"  {name.upper():12s}: Error - {e}")
            results[name] = 0.0

    return results


# ─────────────────────────────────────────────────────────────
# HYPERPARAMETER TUNING WITH CROSS-VALIDATION
# ─────────────────────────────────────────────────────────────
def train_with_cv(estimator, param_grid, train_df, model_name):
    """
    Train a model with k-fold CrossValidation for hyperparameter tuning.
    Returns: (best_model, training_time)
    """
    evaluator = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="f1"
    )

    crossval = CrossValidator(
        estimator=estimator,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=NUM_FOLDS,
        parallelism=2,
        seed=42
    )

    print(f"\n[{datetime.now()}] Training {model_name} with {NUM_FOLDS}-fold CV...")
    print(f"  Parameter combinations: {len(param_grid)}")

    t0 = time.time()
    cv_model = crossval.fit(train_df)
    train_time = time.time() - t0

    print(f"  Training completed in {train_time:.1f}s")
    print(f"  Best F1 (CV avg): {max(cv_model.avgMetrics):.4f}")

    return cv_model.bestModel, round(train_time, 2)


# ─────────────────────────────────────────────────────────────
# MLflow TRACKING (lightweight wrapper)
# ─────────────────────────────────────────────────────────────
def log_to_mlflow(model_name, metrics, params, train_time):
    """
    Log experiment results. Uses MLflow if available, otherwise
    falls back to local JSON logging for reproducibility.
    """
    experiment = {
        "run_id": f"{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "model_name": model_name,
        "metrics": metrics,
        "params": params,
        "train_time_sec": train_time,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        import mlflow
        mlflow.set_experiment("sentiment-analysis")
        with mlflow.start_run(run_name=model_name):
            for k, v in metrics.items():
                mlflow.log_metric(k, v)
            for k, v in params.items():
                mlflow.log_param(k, str(v))
            mlflow.log_metric("train_time_sec", train_time)
        print(f"  [MLflow] Logged run for {model_name}")
    except ImportError:
        print(f"  [MLflow] Not installed – using local tracking")
    except Exception as e:
        print(f"  [MLflow] Tracking failed: {e} – using local tracking")

    return experiment


# ─────────────────────────────────────────────────────────────
# SAVE MODEL TO HDFS (Model Registry)
# ─────────────────────────────────────────────────────────────
def save_model(model, model_name):
    """Save trained model to HDFS for versioning and deployment."""
    safe_name = model_name.lower().replace(" ", "_")
    model_path = f"{MODEL_SAVE_PATH}/{safe_name}"
    try:
        model.write().overwrite().save(model_path)
        print(f"  [Model Registry] Saved {model_name} → {model_path}")
        return model_path
    except Exception as e:
        print(f"  [Model Registry] Warning: Could not save model. Error: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# SAVE TO MONGODB (Predictions / Scores / Metadata)
# ─────────────────────────────────────────────────────────────
def save_to_mongodb(all_results, best_predictions_df, best_model_name):
    """
    Store results to MongoDB (as shown in diagram):
    - model_metrics collection: accuracy, precision, recall, f1, auc per model
    - predictions collection: batch predictions with scores
    - model_metadata collection: best model info, versioning
    """
    try:
        from pymongo import MongoClient
        print(f"\n[{datetime.now()}] Saving results to MongoDB...")

        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]

        # ── 1. Model Metrics ────────────────────────────────
        metrics_col = db["model_metrics"]
        metrics_col.delete_many({})

        metrics_docs = []
        for name, res in all_results["models"].items():
            metrics_docs.append({
                "model_name": name,
                "accuracy": res["accuracy"],
                "precision": res["precision"],
                "recall": res["recall"],
                "f1_score": res["f1"],
                "auc_roc": res["auc"],
                "train_time_sec": res["train_time"],
                "is_best": (name == best_model_name),
                "timestamp": all_results["timestamp"],
                "version": all_results.get("version", "1.0")
            })
        metrics_col.insert_many(metrics_docs)
        print(f"  [MongoDB] Saved {len(metrics_docs)} model metrics")

        # ── 2. Predictions / Scores ─────────────────────────
        pred_col = db["predictions"]
        pred_col.delete_many({})

        pred_rows = best_predictions_df.select(
            "id", "text", LABEL_COL, "prediction", "rawPrediction"
        ).limit(5000).collect()

        pred_docs = []
        for row in pred_rows:
            pred_docs.append({
                "id": str(row["id"]),
                "text": row["text"],
                "actual_label": int(row[LABEL_COL]),
                "predicted_label": int(row["prediction"]),
                "raw_scores": row["rawPrediction"].toArray().tolist(),
                "model_name": best_model_name,
                "timestamp": all_results["timestamp"]
            })

        if pred_docs:
            pred_col.insert_many(pred_docs)
        print(f"  [MongoDB] Saved {len(pred_docs)} predictions")

        # ── 3. Model Metadata ───────────────────────────────
        meta_col = db["model_metadata"]
        meta_col.delete_many({})
        meta_col.insert_one({
            "best_model": best_model_name,
            "best_f1": all_results["models"][best_model_name]["f1"],
            "best_accuracy": all_results["models"][best_model_name]["accuracy"],
            "total_models_trained": len(all_results["models"]),
            "cv_folds": NUM_FOLDS,
            "feature_column": FEATURE_COL,
            "version": all_results.get("version", "1.0"),
            "timestamp": all_results["timestamp"],
            "pipeline": "Layer 3 - Machine Learning"
        })
        print(f"  [MongoDB] Saved model metadata")

        client.close()
        print(f"  [MongoDB] All results saved successfully ✓")

    except ImportError:
        print(f"  [MongoDB] pymongo not installed. Skipping MongoDB save.")
    except Exception as e:
        print(f"  [MongoDB] Warning: Could not save to MongoDB. Error: {e}")


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────
def main():
    start = time.time()
    spark = create_spark_session("Sentiment-ML-Layer3")
    mlflow_runs = []

    print(f"\n{'#'*60}")
    print(f"#  LAYER 3: MACHINE LEARNING PIPELINE")
    print(f"#  Models: Random Forest | SVM | MLP (LSTM proxy)")
    print(f"#  Features: Hyperparameter Tuning, MLflow, Model Registry")
    print(f"{'#'*60}")

    # ─────────────────────────────────────────────────────────
    # LOAD DATA & DOWNSAMPLE (Giảm dung lượng cho máy yếu)
    # ─────────────────────────────────────────────────────────
    print(f"\n[{datetime.now()}] Loading feature data from HDFS (sampled for weak machines)...")
    # Lấy 5% dữ liệu train và 10% dữ liệu test để chạy được trên máy RAM thấp
    train_df = spark.read.parquet(TRAIN_PATH).sample(fraction=0.05, seed=42)
    test_df = spark.read.parquet(TEST_PATH).sample(fraction=0.1, seed=42)

    train_count = train_df.count()
    test_count = test_df.count()
    print(f"  Train samples (5%): {train_count:,}")
    print(f"  Test samples (10%):  {test_count:,}")

    # ─────────────────────────────────────────────────────────
    # MODEL 1: RANDOM FOREST
    # Algorithm Selection → Training, Validation & Hyperparameter Tuning
    # ─────────────────────────────────────────────────────────
    rf = RandomForestClassifier(
        featuresCol=FEATURE_COL,
        labelCol=LABEL_COL,
        seed=42
    )

    rf_param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [20, 50]) \
        .addGrid(rf.maxDepth, [5]) \
        .addGrid(rf.minInstancesPerNode, [2]) \
        .build()

    rf_model, rf_time = train_with_cv(rf, rf_param_grid, train_df, "Random Forest")
    rf_preds = rf_model.transform(test_df)
    rf_results = evaluate_model(rf_preds, "Random Forest")
    rf_results["train_time"] = rf_time

    rf_params = {
        "numTrees": rf_model.getNumTrees,
        "maxDepth": rf_model.getOrDefault("maxDepth"),
    }
    mlflow_runs.append(log_to_mlflow("Random Forest", rf_results, rf_params, rf_time))
    save_model(rf_model, "Random Forest")

    # ─────────────────────────────────────────────────────────
    # MODEL 2: LINEAR SVM (Support Vector Machine)
    # ─────────────────────────────────────────────────────────
    svm = LinearSVC(
        featuresCol=FEATURE_COL,
        labelCol=LABEL_COL
    )

    svm_param_grid = ParamGridBuilder() \
        .addGrid(svm.maxIter, [10, 20]) \
        .addGrid(svm.regParam, [0.1]) \
        .build()

    svm_model, svm_time = train_with_cv(svm, svm_param_grid, train_df, "Linear SVM")
    svm_preds = svm_model.transform(test_df)
    svm_results = evaluate_model(svm_preds, "Linear SVM")
    svm_results["train_time"] = svm_time

    svm_params = {
        "maxIter": svm_model.getOrDefault("maxIter"),
        "regParam": svm_model.getOrDefault("regParam"),
    }
    mlflow_runs.append(log_to_mlflow("Linear SVM", svm_results, svm_params, svm_time))
    save_model(svm_model, "Linear SVM")

    # ─────────────────────────────────────────────────────────
    # MODEL 3: MULTILAYER PERCEPTRON (LSTM proxy)
    # PySpark MLlib does not support LSTM natively;
    # MLP is the closest deep learning classifier available.
    # ─────────────────────────────────────────────────────────
    # Determine input dimension from features
    # Use word2vec_features for MLP instead of TF-IDF to avoid OOM!
    MLP_FEATURE_COL = "word2vec_features"
    sample_row = train_df.select(MLP_FEATURE_COL).head()
    input_dim = len(sample_row[0])

    # MLP architecture: input → hidden → output(2 classes)
    layers = [input_dim, 16, 2]

    mlp = MultilayerPerceptronClassifier(
        featuresCol=MLP_FEATURE_COL,
        labelCol=LABEL_COL,
        layers=layers,
        seed=42
    )

    mlp_param_grid = ParamGridBuilder() \
        .addGrid(mlp.maxIter, [10, 20]) \
        .addGrid(mlp.blockSize, [32]) \
        .build()

    mlp_model, mlp_time = train_with_cv(mlp, mlp_param_grid, train_df, "MLP (LSTM proxy)")
    mlp_preds = mlp_model.transform(test_df)
    
    # Rename word2vec_features prediction back to standard for evaluator?
    # Evaluator just uses labelCol and predictionCol, so it's fine.
    mlp_results = evaluate_model(mlp_preds, "MLP (LSTM proxy)")
    mlp_results["train_time"] = mlp_time

    mlp_params = {
        "layers": str(layers),
        "maxIter": mlp_model.getOrDefault("maxIter"),
        "blockSize": mlp_model.getOrDefault("blockSize"),
    }
    mlflow_runs.append(log_to_mlflow("MLP (LSTM proxy)", mlp_results, mlp_params, mlp_time))
    save_model(mlp_model, "MLP_LSTM")

    # ─────────────────────────────────────────────────────────
    # SELECT BEST MODEL
    # ─────────────────────────────────────────────────────────
    all_models = [
        ("Random Forest", rf_results, rf_preds),
        ("Linear SVM", svm_results, svm_preds),
        ("MLP (LSTM proxy)", mlp_results, mlp_preds),
    ]

    best_name, best_results, best_preds = max(all_models, key=lambda x: x[1]["f1"])

    print(f"\n{'='*60}")
    print(f"  ★ BEST MODEL: {best_name}")
    print(f"    F1-Score : {best_results['f1']:.4f}")
    print(f"    Accuracy : {best_results['accuracy']:.4f}")
    print(f"    AUC-ROC  : {best_results['auc']:.4f}")
    print(f"{'='*60}")

    # ─────────────────────────────────────────────────────────
    # MODEL DEPLOYMENT: BATCH PREDICTION
    # Save predictions to HDFS for REST API / downstream use
    # ─────────────────────────────────────────────────────────
    print(f"\n[{datetime.now()}] Model Deployment: Saving batch predictions...")

    best_preds.select(
        "id", "text", LABEL_COL, "prediction", "rawPrediction"
    ).write.mode("overwrite").parquet(PREDICTIONS_HDFS_PATH)

    print(f"  Predictions saved → {PREDICTIONS_HDFS_PATH}")

    # ─────────────────────────────────────────────────────────
    # SAVE METRICS TO HDFS (versioned)
    # ─────────────────────────────────────────────────────────
    all_results = {
        "models": {
            "Random Forest": rf_results,
            "Linear SVM": svm_results,
            "MLP (LSTM proxy)": mlp_results,
        },
        "best_model": best_name,
        "best_f1": best_results["f1"],
        "cv_folds": NUM_FOLDS,
        "version": "1.0",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mlflow_runs": mlflow_runs,
    }

    # Save metrics JSON to local, then HDFS
    local_metrics_path = "/tmp/metrics.json"
    with open(local_metrics_path, "w") as f:
        json.dump(all_results, f, indent=4, default=str)

    import subprocess
    try:
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", f"{MODEL_SAVE_PATH}"],
            check=False
        )
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_metrics_path, METRICS_HDFS_PATH],
            check=True
        )
        print(f"[{datetime.now()}] Metrics saved to HDFS: {METRICS_HDFS_PATH}")
    except Exception as e:
        print(f"[{datetime.now()}] Warning: HDFS CLI unavailable. Error: {e}")

    # ─────────────────────────────────────────────────────────
    # SAVE TO MONGODB (Predictions / Scores / Metadata)
    # As shown in diagram: Store Results to MongoDB
    # ─────────────────────────────────────────────────────────
    save_to_mongodb(all_results, best_preds, best_name)

    # ─────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────
    elapsed = time.time() - start

    print(f"\n{'#'*60}")
    print(f"#  LAYER 3 PIPELINE COMPLETE")
    print(f"#")
    print(f"#  Models Trained:  3 (Random Forest, SVM, MLP)")
    print(f"#  Best Model:      {best_name}")
    print(f"#  Best F1:         {best_results['f1']:.4f}")
    print(f"#  CV Folds:        {NUM_FOLDS}")
    print(f"#  Total Time:      {elapsed:.1f}s")
    print(f"#")
    print(f"#  Outputs:")
    print(f"#    HDFS Models:     {MODEL_SAVE_PATH}/")
    print(f"#    HDFS Predictions: {PREDICTIONS_HDFS_PATH}")
    print(f"#    HDFS Metrics:    {METRICS_HDFS_PATH}")
    print(f"#    MongoDB:         {MONGODB_DB}")
    print(f"{'#'*60}")

    spark.stop()
    return elapsed


if __name__ == "__main__":
    main()
    sys.exit(0)