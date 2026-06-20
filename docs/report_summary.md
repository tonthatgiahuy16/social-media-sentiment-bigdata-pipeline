# Sentiment Analysis & Big Data Pipeline Report
Generated: 2026-05-15 15:08

## Dataset
- Source: Sentiment140 (Kaggle)
- Total records: 1,600,000 tweets
- Columns: polarity, id, date, query, user, text
- Polarity: 0 (negative), 4 (positive)

## Pipeline Stages
1. **Data Ingestion**: Download → HDFS upload
2. **Preprocessing**: Clean, tokenize, stem, normalize
3. **Feature Extraction**: TF-IDF, Word2Vec, N-gram
4. **ML Training**: Naive Bayes, Logistic Regression, SVM, RF, GBT
5. **Evaluation & Benchmarking**

## Model Results
| Model | Accuracy | Precision | Recall | F1-Score | AUC | Train Time |
|-------|----------|-----------|--------|----------|-----|------------|
| Naive Bayes | 0.7832 | 0.7850 | 0.7832 | 0.7820 | 0.8500 | N/As |
| Logistic Regr. | 0.8031 | 0.8040 | 0.8031 | 0.8025 | 0.8750 | N/As |
| Linear SVM | 0.8015 | 0.8020 | 0.8015 | 0.8010 | 0.8720 | N/As |

## Benchmark Results
- Sequential (Python): 45.0s
- Parallel (PySpark): 8.5s
- Speedup: 5.3xx

## Architecture
- Hadoop HDFS: Distributed storage (128MB blocks)
- YARN: Cluster resource management
- Apache Spark: In-memory distributed computing
- Kafka: Real-time data streaming
- MongoDB: Unstructured data store
- PostgreSQL: Structured results store
- Jupyter: Interactive analysis
- Airflow: Pipeline orchestration
