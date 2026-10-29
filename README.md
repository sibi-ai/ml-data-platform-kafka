# ML Data Platform — Kafka + Feature Store

High-velocity ML data platform for real-time and batch ML workflows.
Kafka-based ingestion, BigQuery ETL, versioned feature store with point-in-time correctness,
and self-service dataset APIs for ML engineers and researchers.

## Architecture

```
Live events  →  Kafka topics  →  Stream processor  →  Feature store
                                         ↓
Batch data   →  BigQuery ETL  →  Training dataset builder
                                         ↓
                            ML engineers / researchers
                            (self-service dataset API)
```

## Features

- Kafka real-time ingestion — millions of events/day, schema validation at ingest
- BigQuery batch ETL for large-scale training dataset generation
- Versioned feature store with point-in-time correctness (no label leakage)
- Self-service dataset generation API for ML teams
- Automated data quality checks at every pipeline stage
- Kubernetes-deployed microservices (GKE / EKS)
- MLflow model registry integration

## Stack

| Component | Technology |
|-----------|-----------|
| Streaming | Apache Kafka, kafka-python |
| Batch ETL | BigQuery, pandas |
| Feature store | Custom (PostgreSQL + versioned snapshots) |
| Orchestration | Kubernetes (GKE), Docker |
| CI/CD | GitHub Actions |
| ML tracking | MLflow |
| Testing | pytest |

## Project Structure

```
├── src/
│   ├── ingestion/      # Kafka consumers and producers
│   ├── etl/            # BigQuery ETL pipelines
│   ├── feature_store/  # Feature writing, versioning, point-in-time reads
│   ├── dataset_api/    # Self-service dataset generation endpoints
│   └── quality/        # Data quality validation
├── configs/
├── tests/
├── k8s/                # Kubernetes manifests
├── .github/workflows/
├── Dockerfile
└── requirements.txt
```

## Quickstart

```bash
git clone https://github.com/sibi-ai/ml-data-platform-kafka
cd ml-data-platform-kafka
pip install -r requirements.txt
# Start Kafka locally
docker compose -f docker-compose.kafka.yml up -d
# Run ingestion consumer
python -m src.ingestion.consumer --topic events --group ml-platform
```
