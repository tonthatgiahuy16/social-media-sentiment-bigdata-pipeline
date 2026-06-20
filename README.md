# Social Media Sentiment Analysis Platform

A scalable Big Data platform for real-time and batch sentiment analysis of social media data using Apache Spark, Hadoop, Kafka, Airflow, FastAPI, and Docker.
## Key Achievements

✅ Processed 1.6 Million Tweets

✅ Apache Spark Distributed Processing

✅ Kafka Real-Time Streaming

✅ 5.3× Faster than Sequential Execution

✅ 429% Throughput Improvement

✅ Automated ETL with Airflow

✅ AUC-ROC ≈ 0.88

✅ Fully Containerized with Docker
## Project Overview

Social media platforms generate massive amounts of user-generated content every day. Extracting meaningful insights from this data requires a scalable and distributed processing architecture.

This project implements an end-to-end Big Data pipeline capable of ingesting, processing, analyzing, and serving sentiment insights from millions of social media posts. The platform supports both batch and streaming workloads and demonstrates modern Data Engineering practices using distributed technologies.

## Resume Highlights

**Role:** Data Engineer / Big Data Developer

### Key Contributions

- Designed and implemented a 4-layer Big Data architecture.
- Built an ETL pipeline for processing 1.6 million tweets.
- Developed distributed sentiment analysis workflows using Apache Spark.
- Implemented real-time streaming with Apache Kafka.
- Automated workflows using Apache Airflow.
- Containerized the entire platform using Docker Compose.
- Built REST APIs for data access and analytics services.

### Technologies

Apache Spark · Hadoop HDFS · Kafka · Airflow · FastAPI · MongoDB · PostgreSQL · Docker · Machine Learning

---

# Key Results

| Metric | Result |
|----------|----------|
| Dataset Size | 1.6 Million Tweets |
| Storage Compression | 3x Reduction (238 MB → 79 MB) |
| Processing Speed Improvement | 5.3x Faster |
| Throughput Improvement | 429% Increase |
| Best Model Performance | AUC-ROC ≈ 0.88 |
| Deployment | Dockerized |

---

# System Architecture

The platform follows a layered architecture designed for scalability and maintainability.

```text
Social Media Data
        │
        ▼
 ┌─────────────────┐
 │ Data Ingestion  │
 │ Kafka Producer  │
 └─────────────────┘
        │
        ▼
 ┌─────────────────┐
 │ Data Storage    │
 │ Hadoop HDFS     │
 └─────────────────┘
        │
        ▼
 ┌─────────────────┐
 │ Data Processing │
 │ Apache Spark    │
 └─────────────────┘
        │
        ▼
 ┌─────────────────┐
 │ Orchestration   │
 │ Apache Airflow  │
 └─────────────────┘
        │
        ▼
 ┌─────────────────┐
 │ Serving Layer   │
 │ FastAPI         │
 └─────────────────┘
        │
        ▼
 PostgreSQL / MongoDB
```

---

# Data Flow

```mermaid
flowchart LR

A[Twitter Dataset] --> B[Kafka Producer]

B --> C[Kafka Topic]

C --> D[Spark Streaming]

D --> E[Data Cleaning]

E --> F[Feature Engineering]

F --> G[Sentiment Classification]

G --> H[HDFS Storage]

H --> I[PostgreSQL]

H --> J[MongoDB]

I --> K[FastAPI]

J --> K

K --> L[Dashboard]
```

---

# Dataset

## Sentiment140 Dataset

Source:

http://help.sentiment140.com/for-students

### Dataset Information

- Approximately 1.6 million tweets
- Binary sentiment classification

| Label | Meaning |
|---------|---------|
| 0 | Negative |
| 4 | Positive |

The dataset is used to evaluate both batch and streaming sentiment analysis pipelines.

---

# Technology Stack

## Data Engineering

- Apache Spark
- Hadoop HDFS
- Apache Kafka
- Apache Airflow

## Backend Development

- FastAPI
- Python

## Databases

- PostgreSQL
- MongoDB

## Infrastructure

- Docker
- Docker Compose

## Machine Learning

- Scikit-learn
- Logistic Regression
- Random Forest
- Support Vector Machine

---

# Project Workflow

## 1. Data Ingestion

- Load raw social media data
- Stream data using Kafka producers
- Persist raw datasets to HDFS

## 2. Data Processing

Apache Spark performs:

- Text cleaning
- Tokenization
- Stopword removal
- Feature extraction
- Sentiment prediction

## 3. Workflow Orchestration

Apache Airflow manages:

- ETL scheduling
- Data preprocessing
- Model execution
- Data loading

## 4. Storage Layer

### Hadoop HDFS

Stores:

- Raw datasets
- Processed datasets
- Model outputs

### PostgreSQL

Stores:

- Structured analytical results
- Aggregated metrics

### MongoDB

Stores:

- Semi-structured sentiment records
- Streaming results

## 5. API Layer

FastAPI provides:

- Analytics endpoints
- Sentiment queries
- Reporting services

---

# Machine Learning Pipeline

## Data Preprocessing

- Text normalization
- URL removal
- Special character removal
- Stopword filtering

## Feature Engineering

- TF-IDF Vectorization

## Models Evaluated

- Logistic Regression
- Random Forest
- Support Vector Machine (SVM)

## Best Performance

| Metric | Score |
|----------|----------|
| AUC-ROC | 0.88 |
| Dataset Size | 1.6M Tweets |

---

# Repository Structure

```text
social-media-sentiment-bigdata-pipeline
│
├── airflow/
│   └── dags/
│
├── api/
│
├── dashboard/
│
├── docs/
│
├── hadoop-config/
│
├── notebooks/
│
├── scripts/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── training/
│   └── streaming/
│
├── spark-config/
│
├── docker-compose.yml
│
├── run_pipeline.sh
│
└── README.md
```

---

# Analytics & Visualization

The platform provides multiple analytical views for monitoring sentiment trends, model performance, and real-time processing results.

## Sentiment Distribution

Distribution of positive and negative sentiments across the dataset.

<p align="center">
  <img src="docs/assets/sentiment_distribution.png" width="800">
</p>

---

## Sentiment Trend Analysis

Visualization of sentiment changes over time.

<p align="center">
  <img src="docs/assets/sentiment_trend.png" width="800">
</p>

---

## Most Frequent Keywords

Top frequently occurring words extracted from social media posts after preprocessing.

<p align="center">
  <img src="docs/assets/top_words.png" width="800">
</p>

---

## Machine Learning Model Comparison

Performance comparison of evaluated sentiment classification models.

<p align="center">
  <img src="docs/assets/model_comparison.png" width="800">
</p>

---

## Real-Time Streaming Results

Real-time sentiment distribution generated from Kafka + Spark Streaming.

<p align="center">
  <img src="docs/assets/realtime_distribution.png" width="800">
</p>

---

# Performance Benchmark

To evaluate the effectiveness of distributed processing, the Spark implementation was compared against a sequential execution approach.

### Key Findings

* Processing time reduced from **45.0 seconds** to **8.5 seconds**
* Achieved **5.3× speedup**
* Throughput increased from **222 records/second** to **1,176 records/second**
* Overall throughput improvement of **429%**

<p align="center">
  <img src="docs/assets/benchmark_parallel.png" width="1000">
</p>

---

# Installation

## Clone Repository

```bash
git clone https://github.com/tonthatgiahuy16/social-media-sentiment-bigdata-pipeline.git
cd social-media-sentiment-bigdata-pipeline
```

## Start Infrastructure Services

```bash
docker-compose up -d
```

This command launches:

* Hadoop HDFS
* Apache Spark Cluster
* Apache Kafka
* Apache Airflow
* PostgreSQL
* MongoDB

## Run the Data Pipeline

Linux / WSL:

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

Windows PowerShell:

```powershell
.\run_pipeline.sh
```

---

# Future Improvements

Planned enhancements include:

* Deploying the platform on AWS Cloud
* Building a Data Lake architecture using S3
* Integrating MLflow for experiment tracking
* Implementing Kubernetes orchestration
* Adding CI/CD pipelines using GitHub Actions
* Integrating Grafana dashboards for monitoring
* Supporting additional sentiment classes (Neutral, Mixed)
* Real-time alerting for sentiment spikes

---

# Author

## Ton That Gia Huy

Final-Year Data Science Student

### Areas of Interest

* Data Engineering
* Big Data Analytics
* Distributed Systems
* Machine Learning
* Cloud Computing

### Contact

GitHub:
https://github.com/tonthatgiahuy16

LinkedIn:
https://www.linkedin.com/in/t%C3%B4n-th%E1%BA%A5t-gia-huy-708860369/

Email:
mailto:tonthatgiahuy160505@gmail.com

