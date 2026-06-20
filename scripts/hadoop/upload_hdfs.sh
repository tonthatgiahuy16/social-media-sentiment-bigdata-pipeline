#!/bin/bash
# Upload data to HDFS
# Sentiment140 dataset format: polarity(0/4),id,date,query,user,text

set -e

HDFS_DIR="/sentiment"
INPUT_LOCAL="data/raw/sentiment140_full.csv"

echo "=== Upload to HDFS ==="

# Wait for namenode to be ready
echo "Waiting for HDFS namenode..."
sleep 5
docker exec namenode hdfs dfsadmin -safemode leave || true

# Create HDFS directories
docker exec namenode hdfs dfs -mkdir -p $HDFS_DIR/raw
docker exec namenode hdfs dfs -mkdir -p $HDFS_DIR/processed
docker exec namenode hdfs dfs -mkdir -p $HDFS_DIR/test
docker exec namenode hdfs dfs -mkdir -p /spark-history

# Set replication
docker exec namenode hdfs dfs -setrep -w 1 $HDFS_DIR

# Upload raw data
# Note: /data/raw is mapped to ./data/raw in docker-compose.yml
echo "Uploading data from container path /data/raw/ to HDFS..."
docker exec namenode hdfs dfs -put -f /data/raw/sentiment140_full.csv $HDFS_DIR/raw/

# Verify
echo "Verifying HDFS files..."
docker exec namenode hdfs dfs -ls -h $HDFS_DIR/raw/

echo "=== HDFS upload complete ==="
