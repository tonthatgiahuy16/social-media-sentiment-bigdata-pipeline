#!/bin/bash
# Main runner script - executes the full 4-Layer Sentiment Analysis Pipeline
# Layer 1: Storage | Layer 2: Processing | Layer 3: ML | Layer 4: Visualization
set -e

# ══════════════════════════════════════════════════════════════════════════════
# REAL-TIME LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════
mkdir -p logs
LOG_FILE="logs/pipeline_$(date +%Y%m%d_%H%M%S).log"

# stdbuf -oL → line-buffered: ghi log ngay khi có dòng output (real-time)
exec > >(stdbuf -oL tee -a "$LOG_FILE") 2>&1

# Helper: in ra terminal + log kèm timestamp
log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# Python không buffer output → ghi log ngay lập tức
export PYTHONUNBUFFERED=1

log "📄 Log real-time tại: $LOG_FILE"
log "   Bắt đầu: $(date '+%Y-%m-%d %H:%M:%S')"

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Sentiment Analysis Pipeline — 4-Layer Architecture     ║"
echo "║  Storage → Processing → ML → Visualization              ║"
echo "╚══════════════════════════════════════════════════════════╝"

cd "$(dirname "$0")"

# ── STEP 0: Check prerequisites ─────────────────────────────────────────────
echo ""
log "[Step 0] Checking prerequisites..."

check_docker() {
    if ! docker info &>/dev/null; then
        log "ERROR: Docker is not running. Please start Docker first."
        exit 1
    fi
    log "  ✓ Docker is running"
}

check_docker

# Auto-detect docker compose command (plugin vs standalone)
if docker compose version &>/dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &>/dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    log "ERROR: Neither 'docker compose' nor 'docker-compose' found."
    log "Install with: sudo apt install docker-compose"
    exit 1
fi

# ══════════════════════════════════════════════════════════════════════════════
# STEP 0: Start cluster (all services)
# ══════════════════════════════════════════════════════════════════════════════
read -p "Start all services (Hadoop, Spark, Kafka, MongoDB, Airflow, API)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Step 0] Starting 12-service cluster..."
    $DOCKER_COMPOSE up -d
    echo ""
    log "  Services started:"
    echo "    ┌──────────────────────────────────────────────────┐"
    echo "    │ Hadoop NameNode   : http://localhost:9870       │"
    echo "    │ YARN ResourceMgr  : http://localhost:8088       │"
    echo "    │ Spark Master      : http://localhost:8080       │"
    echo "    │ Kafka             : localhost:9092              │"
    echo "    │ MongoDB           : localhost:27017             │"
    echo "    │ Airflow           : http://localhost:8081       │"
    echo "    │ Sentiment API     : http://localhost:8000/docs  │"
    echo "    │ Jupyter Notebook  : http://localhost:8888       │"
    echo "    └──────────────────────────────────────────────────┘"
    echo ""
    log "  Waiting for services to be ready..."
    sleep 15
fi

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1: STORAGE LAYER — Data Ingestion → HDFS (Data Lake)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  LAYER 1: STORAGE LAYER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

read -p "[Layer 1] Upload data to HDFS Data Lake? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 1] Uploading data/raw/ → HDFS /sentiment/raw/"
    bash scripts/hadoop/upload_hdfs.sh
fi

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2: PROCESSING LAYER — Spark on YARN Cluster
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  LAYER 2: PROCESSING LAYER (Spark on YARN)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

read -p "[Layer 2] Run Data Preprocessing (Cleaning, Filtering, Tokenization)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 2 - Step 1] Preprocessing: Clean → Filter → Tokenize..."
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        /data/scripts/spark/preprocessing.py
    log "[Layer 2 - Step 1] ✓ Preprocessing xong"
fi

read -p "[Layer 2] Run Feature Engineering (TF-IDF, Word2Vec, N-gram)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 2 - Step 2] Feature Extraction: TF-IDF + Word2Vec + Bigrams..."
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /data/scripts/spark/feature_extraction.py
    log "[Layer 2 - Step 2] ✓ Feature extraction xong"
fi

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3: MACHINE LEARNING LAYER — Train, Evaluate, Deploy
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  LAYER 3: MACHINE LEARNING LAYER"
echo "  Models: Random Forest | SVM | MLP (LSTM proxy)"
echo "  + Hyperparameter Tuning (CV) + MLflow + Model Registry"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

read -p "[Layer 3] Train ML models (Random Forest, SVM, MLP) with CV tuning? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 3] Training → Evaluation → Deployment → MongoDB..."
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /data/scripts/spark/ml_training.py
    log "[Layer 3] ✓ ML training xong"
fi

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4: VISUALIZATION & REPORTING LAYER
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  LAYER 4: VISUALIZATION & REPORTING LAYER"
echo "  A. Performance Metrics | B. Business Insights"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

read -p "[Layer 4A] Run Benchmark (Parallel vs Sequential)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 4A] Performance: Parallel vs Sequential Benchmark..."
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /data/scripts/spark/benchmark.py
    log "[Layer 4A] ✓ Benchmark xong"
fi

read -p "[Layer 4A] Run Distributed Processing demo? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 4A] Distributed Processing scenarios..."
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /data/scripts/spark/distributed_processing.py
    log "[Layer 4A] ✓ Distributed processing xong"
fi

read -p "[Layer 4B] Generate Visualizations (charts & reports)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo ""
    log "[Layer 4B] Generating: Model Comparison, Sentiment Dist, Trends, Top Words..."
    python -u scripts/visualization.py
    log "[Layer 4B] ✓ Visualizations xong → docs/assets/"
fi

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ✓ All 4 Layers complete!                               ║"
echo "║                                                         ║"
echo "║  Layer 1 (Storage)     → HDFS Data Lake                 ║"
echo "║  Layer 2 (Processing)  → Features extracted              ║"
echo "║  Layer 3 (ML)          → Models trained & deployed       ║"
echo "║  Layer 4 (Viz)         → Charts & reports in docs/       ║"
echo "║                                                         ║"
echo "║  Dashboard: http://localhost:8000/docs                   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
log "✅ Kết thúc: $(date '+%Y-%m-%d %H:%M:%S')"
log "📄 Log đầy đủ real-time đã được lưu tại: $LOG_FILE"
