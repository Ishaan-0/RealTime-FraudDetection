# Real-Time Credit Card Fraud Detection System

## 🔹 Value
- Detects suspicious credit card transactions **in real time** using **ensemble ML models** (Random Forest + XGBoost).  
- High accuracy + low latency through **feature engineering** and **efficient model pipelines**.  
- Demonstrates an **end-to-end ML system**:
  - Data streaming
  - Big data processing
  - Model serving
  - Alerting
  - Containerized infra  
- Showcases **production skills**:
  - Kafka-based ingestion
  - Spark micro-batching
  - Ensemble model pipelines
  - Extensible alert routing  

---

## 🔹 System Architecture
- Data flows from a **streaming producer** → **Kafka topics** → **Spark Structured Streaming consumer**.  
- Real-time **feature engineering** + **ML inference** → generates **fraud probabilities and risk levels**.  
- Alerts written to storage/logs for analyst triage.  
- **Checkpointing** for fault tolerance, **micro-batches** for predictable latency.  
- Model artifacts stored in a dedicated directory.  

**High-level flow:**
Producer → Kafka Topic (partitioned) → Spark Consumer → Feature Engineering → ML Inference → Risk Decision → Alerts/Output

---

## 🔹 Kafka and Zookeeper Architecture
- **Kafka cluster** of brokers, each handling topic partitions for scalability & parallelism.  
- Producers write in parallel; consumer groups distribute partitions for balanced processing.  
- **Zookeeper**:
  - Manages broker metadata
  - Coordinates partition leadership
  - Tracks broker liveness  
- Ensures resilience + failover behavior.  

**Key concepts:**
- Producers publish JSON events to topic (e.g., `txns`).  
- Brokers persist messages with ordered offsets.  
- Consumers scale horizontally by partitions.  
- Zookeeper ensures controller election & cluster health.  

---

## 🔹 ML Models
- **Problem:** Highly imbalanced classification (fraud vs. legitimate).  
- **Models:** Random Forest + XGBoost in ensemble.  
  - Pipelines with imputation & scaling.  
  - SMOTE used during training for class imbalance.  
- **Inference:**  
  - Predict fraud probability per transaction.  
  - Simple average ensemble for robustness.  
- **Features:**  
  - PCA-like `V1–V28`  
  - Engineered features: `log(amount)`, outlier indicators  
  - Real-time data cleaning (replace inf/-inf, fill missing).  
- **Outputs:**  
  - `fraud_probability`  
  - `is_fraud` flag  
  - Risk levels: **LOW / MEDIUM / HIGH** (threshold-based)  

---

## 🔹 Producer and Consumer

### Producer
- Reads CSV transactions, cleans unnamed cols.  
- Assigns unique `transaction_id` + timestamp.  
- Serializes to JSON and streams to Kafka.  
- Configurable rate (events/sec).  
- Batching + acknowledgements + periodic flush.  

### Consumer
- Subscribes to Kafka topic; processes in micro-batches via Spark.  
- Validates + parses JSON, performs feature engineering.  
- Ensures all model features exist.  
- Converts to Pandas for inference.  
- Computes fraud probability → risk decision.  
- Writes alerts to files for downstream use.  
- Checkpointing + triggers ensure consistency + latency control.  

---

## 🔹 Future Improvements

### Streaming and Infra
- Multi-broker Kafka, higher replication factor.  
- More partitions for producer/consumer parallelism.  
- Spark on Kubernetes/YARN with autoscaling.  
- Schema Registry for schema evolution & validation.  

### Model Serving and MLOps
- Serve models via MLflow/TensorFlow Serving.  
- Add A/B testing + shadow deployments.  
- Drift detection, performance monitoring, automated retraining.  
- Optimize models with feature selection, quantization.  

### Storage and Feature Serving
- Low-latency feature store (Redis/Cassandra).  
- Centralized alert store (MongoDB/Postgres).  
- Analyst workflow integration.  

### Observability and Reliability
- Metrics (Prometheus/Grafana): throughput, latency (P95/P99), error rates, consumer lag.  
- Dead-letter topics, retry/backoff strategies.  
- Robust security: TLS, SASL, secrets management, audit logs.  

---

## 🚀 Skills Showcased
- **Big Data:** Kafka, Spark Structured Streaming  
- **ML Engineering:** Ensemble models, feature engineering, SMOTE  
- **MLOps:** Checkpointing, artifacts, retraining paths  
- **Systems:** Micro-batching, containerization, observability, security  
- **Practical Value:** Real-time fraud detection with actionable alerts
