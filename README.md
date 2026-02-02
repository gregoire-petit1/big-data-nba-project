# Big Data NBA Project

A complete big data pipeline for NBA analytics using Apache Spark, Apache Airflow, MinIO (S3-compatible storage), and Elasticsearch with Kibana.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │────▶│   Ingestion │────▶│  Data Lake  │────▶│   Spark     │
│  (APIs)     │     │  (Python)   │     │   (MinIO)   │     │(Processing) │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                             ┌─────────────┐     ┌─────────────┐   │
                             │   Kibana    │◀────│Elasticsearch│◀──┘
                             │ (Dashboard) │     │  (Index)    │
                             └─────────────┘     └─────────────┘

                             ┌─────────────┐
                             │   Airflow   │
                             │  (Orchestr.)│
                             └─────────────┘
```

## Data Sources

- **balldontlie API**: NBA games and teams data
- **TheSportsDB API**: Team details and venue information

## Tech Stack

- **Apache Spark**: Data processing and ML
- **Apache Airflow**: Pipeline orchestration
- **MinIO**: S3-compatible object storage (Data Lake)
- **Elasticsearch**: Data indexing and search
- **Kibana**: Data visualization and dashboards
- **PostgreSQL**: Airflow metadata database
- **Docker**: Containerization

## Project Structure

```
big-data-nba-project/
├── config/                 # Configuration files
├── dags/                   # Airflow DAGs
│   └── nba_pipeline_dag.py
├── docker/                 # Docker configurations
│   ├── airflow/
│   └── spark/
├── ingestion/              # Data ingestion scripts
│   ├── ingest_balldontlie.py
│   └── ingest_thesportsdb.py
├── jobs/                   # Spark jobs
│   ├── combine_metrics.py
│   ├── format_balldontlie.py
│   ├── format_thesportsdb.py
│   ├── index_match_metrics.py
│   ├── index_team_metrics.py
│   ├── spark_utils.py
│   └── train_predict.py
├── docker-compose.yml      # Docker services orchestration
├── .env.example            # Environment variables template
└── README.md               # This file
```

## Data Lake Structure

```
datalake/
├── raw/                    # Raw API data
│   └── nba/
│       ├── balldontlie/
│       │   ├── games/
│       │   └── teams/
│       └── thesportsdb/
│           └── teams/
├── formatted/              # Cleaned and typed data (Parquet)
│   └── nba/
│       ├── balldontlie/
│       │   ├── games/
│       │   └── teams/
│       └── thesportsdb/
│           └── teams/
└── combined/               # Aggregated metrics
    └── nba/
        ├── match_metrics/
        └── team_metrics/
```

## Prerequisites

- Docker and Docker Compose
- Git
- API keys:
  - [balldontlie API](https://www.balldontlie.io/) - Free tier available
  - [TheSportsDB API](https://www.thesportsdb.com/api.php) - Free tier available

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/gregoire-petit1/big-data-nba-project.git
cd big-data-nba-project
```

### 2. Configure Environment Variables

```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your API keys
nano .env  # or use your preferred editor
```

**Required variables:**

```env
# API keys (get from the respective websites)
BALDONTLIE_API_KEY=your_balldontlie_api_key_here
THESPORTSDB_API_KEY=your_thesportsdb_api_key_here

# MinIO (default values are fine for local dev)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=datalake

# Elasticsearch
ELASTIC_HOST=http://elasticsearch
ELASTIC_PORT=9200

# API bases (don't change unless APIs move)
BALDONTLIE_API_BASE=https://api.balldontlie.io/v1
THESPORTSDB_API_BASE=https://www.thesportsdb.com/api/v1/json
```

### 3. Start the Infrastructure

```bash
docker-compose up -d
```

This will start:

- Airflow (webserver + scheduler) - http://localhost:8080
- Spark (master + worker) - http://localhost:8081
- MinIO - http://localhost:9000 (console: http://localhost:9001)
- Elasticsearch - http://localhost:9200
- Kibana - http://localhost:5601
- PostgreSQL (Airflow metadata)

**Default Airflow credentials:**

- Username: `admin`
- Password: `admin`

> ⚠️ **Security Note**: These are default credentials for local development only. Change `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` in `.env` before deploying to production.

### 4. Run the Pipeline

The pipeline runs automatically via Airflow DAG. To trigger manually:

1. Open Airflow UI: http://localhost:8080
2. Login with admin/admin
3. Find the DAG `nba_pipeline`
4. Toggle the switch to enable it
5. Click "Trigger DAG" (play button)

Or trigger via CLI:

```bash
docker-compose exec airflow-webserver airflow dags trigger nba_pipeline
```

### 5. Access the Dashboard

Once the pipeline completes:

1. Open Kibana: http://localhost:5601
2. Create index patterns:
   - `nba_team_metrics`
   - `nba_match_metrics`
3. Build visualizations and dashboards

## Pipeline Stages

1. **Ingestion**: Fetch data from APIs and store in MinIO (raw layer)
2. **Formatting**: Clean and convert to Parquet (formatted layer)
3. **Combination**: Join datasets and compute KPIs (combined layer)
4. **ML Prediction**: Train logistic regression model to predict win probability
5. **Indexing**: Load data into Elasticsearch for visualization

## Key Metrics

- **Team Metrics**: Win rate, average points, home/away performance
- **Match Metrics**: Win probability, rest days, recent form

## Development

### Running Spark Jobs Locally

```bash
# Format balldontlie data
docker-compose exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/format_balldontlie.py

# Combine metrics
docker-compose exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/combine_metrics.py
```

### Running Ingestion Scripts

```bash
# Ingest balldontlie data
docker-compose exec airflow-webserver python /opt/airflow/ingestion/ingest_balldontlie.py --seasons 2022 2023 2024

# Ingest TheSportsDB data
docker-compose exec airflow-webserver python /opt/airflow/ingestion/ingest_thesportsdb.py --league NBA
```

### Accessing MinIO

- URL: http://localhost:9001
- Username: minioadmin (or your MINIO_ROOT_USER)
- Password: minioadmin (or your MINIO_ROOT_PASSWORD)

## Troubleshooting

### Services won't start

```bash
# Check logs
docker-compose logs <service-name>

# Restart specific service
docker-compose restart <service-name>

# Full reset (WARNING: deletes all data)
docker-compose down -v
docker-compose up -d
```

### Pipeline fails

1. Check Airflow logs in the UI
2. Verify API keys are set in `.env`
3. Ensure all services are healthy: `docker-compose ps`

### No data in Elasticsearch

1. Check Spark jobs completed successfully
2. Verify Elasticsearch is running: `curl http://localhost:9200/_cluster/health`
3. Check index exists: `curl http://localhost:9200/_cat/indices`

## API Documentation

- **balldontlie**: https://www.balldontlie.io/
- **TheSportsDB**: https://www.thesportsdb.com/api.php

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -am 'Add new feature'`
4. Push to branch: `git push origin feature/my-feature`
5. Submit a pull request

## License

This project is for educational purposes. Please respect the API terms of service.

## Acknowledgments

- NBA data provided by balldontlie and TheSportsDB
- Built with Apache Spark, Airflow, and the Elastic Stack
