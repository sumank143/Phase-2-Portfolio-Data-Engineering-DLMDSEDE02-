# Real-Time Accident Severity — Data Engineering

**Course:** Data Engineering (DLMDSEDE02) — IU Internationale Hochschule
**Task:** Build a real-time data backend for a data-intensive application

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Microservices](#3-microservices)
4. [Data Source](#4-data-source)
5. [Reliability, Scalability & Maintainability](#5-reliability-scalability--maintainability)
6. [Data Security, Governance & Protection](#6-data-security-governance--protection)
7. [Docker Images](#7-docker-images)
8. [Aggregation & Windowing](#8-aggregation--windowing)
9. [Infrastructure as Code](#9-infrastructure-as-code)
10. [Local Quick-Start](#10-local-quick-start)
11. [Cloud Deployment](#11-cloud-deployment)
12. [Project Reflection (Finalization Phase)](#12-project-reflection-finalization-phase)

---

## 1. Project Overview

This project implements a real-time streaming data backend for a road-accident severity analytics dashboard. The system continuously ingests two static CSV datasets (replayed as a time-indexed event stream), processes the stream with five parallel Apache Spark Structured Streaming jobs, materialises aggregated results into a Postgres database, retrains an ML severity classifier every 15 minutes, and delivers live data to a browser dashboard via WebSocket.

**Key numbers:**
- `Accident_Information.csv` — 2,047,256 rows (timestamped with `Date` + `Time`)
- `Vehicle_Information.csv` — 2,177,205 rows (joined to accidents via `Accident_Index`)
- Producer throughput: configurable 100–500 msg/s per stream, unthrottled mode also available
- Spark micro-batch trigger: every 1 minute
- ML retrain schedule: every 15 minutes (Glue cron trigger)
- Dashboard WebSocket push: every 10 seconds

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  AWS (EC2 / S3 / Glue / RDS / Secrets Manager)                       │
│                                                                        │
│  ┌───────────┐     SASL_SSL/Kafka     ┌─────────────────────────────┐ │
│  │  producer │ ───────────────────►  │  Azure Event Hubs            │ │
│  │  (EC2)    │  accident-raw          │  (Kafka surface)             │ │
│  │           │  vehicles-raw          │  Standard, 4 partitions/topic│ │
│  └───────────┘                        └──────────┬──────────────────┘ │
│       │ reads CSVs                               │ Kafka consumer      │
│       ▼                                          ▼                     │
│  ┌─────────┐              ┌──────────────────────────────────────────┐ │
│  │  AWS S3  │◄────────────│  AWS Glue Streaming (5 PySpark jobs)    │ │
│  │  - raw/  │  checkpoint  │  job1: accident_kpi_geo                  │ │
│  │  - proc/ │  + parquet   │  job2: accident_conditions               │ │
│  │  - models│             │  job3: accident_hotspots                 │ │
│  └─────────┘              │  job4: vehicle_profile                   │ │
│       │  ▲                │  job5: extract → accidents/ + vehicles/  │ │
│       │  │parquet         └─────────────────────┬────────────────────┘ │
│       │  │(job5)                                │ UPSERT every 1 min   │
│       │  │reads                                 ▼                     │
│       │  │parquet         ┌──────────────────────────────────────────┐ │
│       │  │  ┌─────────────┤  RDS Postgres (4 tables)                │ │
│       │  │  │  Glue ML    │  public schema                           │ │
│       │  └──┤  Train      └──────────────┬───────────────────────────┘ │
│       │     │  every 15m                 │ reads                       │
│       └────►│  → model.joblib            │                             │
│             └────────────────────────────┘                             │
└──────────────────────────────────────────────────────┼─────────────────┘
                                                        │
                    ┌───────────────────────────────────┼──────────────┐
                    │  Local / Docker Compose           │              │
                    │                                   ▼              │
                    │  ┌────────────┐      ┌────────────────────────┐  │
                    │  │ ml-predict │◄─────│     dashboard          │  │
                    │  │ :8001      │ HTTP │     :8000              │  │
                    │  │ FastAPI    │      │     FastAPI + WS       │──┼──► Browser
                    │  │ loads      │      │     10-sec push        │  │
                    │  │ model from │      └────────────────────────┘  │
                    │  │ S3         │                                   │
                    │  └────────────┘                                   │
                    └──────────────────────────────────────────────────┘
```

**Cross-cloud note:** Event Hubs (Kafka) lives in Azure because Azure for Students offers a free Standard-tier namespace; all other resources (EC2, S3, RDS, Glue) are on AWS Free Tier. This is intentional and managed entirely by Terraform.

---

## 3. Microservices

The system is decomposed into five independently deployable microservices. Each has its own Dockerfile and no shared process state.

### 3.1 Producer (data ingestion)

**Container:** `accident-severity/producer`
**Code:** [producer/producer.py](producer/producer.py)

Runs two child processes (one per stream) using Python `multiprocessing`. Each process:
1. Opens an S3 object for the relevant CSV as a streaming HTTP body (`iter_lines`) — no full file in memory.
2. Parses CSV rows as dicts, wraps each in a typed envelope (`seq`, `stream`, `event_time`, `ingest_time`, `payload`), and produces to an Azure Event Hubs topic via the Kafka protocol.
3. Uses `acks=all`, `retries=5`, `linger_ms=50`, and a fixed `api_version=(2,0,0)` (Event Hubs rejects newer Kafka protocol features).
4. SASL_SSL + PLAIN authentication — connection string is injected via `KAFKA_SASL_PASSWORD` environment variable, never written to disk.

Topics produced to:
- `accident-raw` — keyed by `Accident_Index`
- `vehicles-raw` — keyed by `Accident_Index` (correlates vehicles to accidents downstream)

The `Accident_Index` key keeps all vehicles for one accident on the same Kafka partition, which is the prerequisite for deterministic join correctness in job5.

### 3.2 AWS Glue Streaming (stream processing & aggregation)

**Code:** [infra/glue_scripts/](infra/glue_scripts/)

Five long-running PySpark Structured Streaming jobs, each provisioned as a separate Glue job. They share `common.py` and `schemas.py` (uploaded to S3 as `--extra-py-files`).

| Job | Input topic | Output | Aggregation key |
|-----|-------------|--------|-----------------|
| job1_accident_kpi_geo | accident-raw | Postgres `accident_kpi_geo` | event_date, lat_grid, lon_grid |
| job2_accident_conditions | accident-raw | Postgres `accident_conditions` | event_date, weather, light, road_surface, speed_limit |
| job3_accident_hotspots | accident-raw | Postgres `accident_hotspots` | event_date, local_authority_district, road_type, urban_or_rural |
| job4_vehicle_profile | vehicles-raw | Postgres `vehicle_profile` | year, age_band_of_driver, sex_of_driver, vehicle_type |
| job5_accident_vehicle_join | accident-raw + vehicles-raw | S3 `processed/accidents/` + S3 `processed/vehicles/` | — (raw extract, no aggregation) |

Jobs 1–4 share the **stateless foreachBatch + READ-MERGE-WRITE UPSERT** pattern (see [Section 8](#8-aggregation--windowing)). Job 5 is a pure extraction job — no aggregation, no Postgres write.

### 3.3 AWS Glue ML Training (model lifecycle)

**Code:** [ml_accidental_severity/train.py](ml_accidental_severity/train.py)

A Glue Python Shell job (0.0625 DPU) scheduled every 15 minutes by a Glue Trigger. It:
1. Reads accident parquet from `s3://<bucket>/processed/accidents/` (written by job5).
2. Reads vehicle parquet from `s3://<bucket>/processed/vehicles/` (written by job5).
3. Joins both on `Accident_Index` in Pandas — no stream-stream join complexity.
4. Cleans rows with missing/unknown feature values.
5. Trains a `sklearn` `Pipeline` — `OneHotEncoder` → `LogisticRegression(class_weight="balanced")`.
6. Serialises the pipeline plus feature options and accuracy metadata with `joblib` and writes `models/severity_classifier.joblib` to S3.

The training job is infrastructure-managed (not a Docker container) because it is a scheduled job, not a long-lived service.

**Local bootstrap:** If the Glue pipeline has not yet produced enough parquet, run [ml_accidental_severity/train_local.py](ml_accidental_severity/train_local.py) to train directly from the local CSVs and upload the model to S3:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
python ml_accidental_severity/train_local.py
docker compose restart ml-predict
```

### 3.4 ML Predict (inference microservice)

**Container:** `accident-severity/ml-predict`
**Code:** [ml_predict/app/main.py](ml_predict/app/main.py), [ml_predict/app/predict.py](ml_predict/app/predict.py)

Stateless FastAPI service on port 8001. On each `/predict` call it:
1. Checks a 5-minute in-process cache; if stale, fetches `severity_classifier.joblib` from S3 and deserialises it.
2. Runs `pipeline.predict_proba` for the three input features (`age_band_of_driver`, `sex_of_driver`, `vehicle_type`).
3. Returns a ranked list of `{severity, probability}` pairs.

The model cache TTL is configurable via `MODEL_CACHE_TTL_SEC`. The service is completely stateless — any restart automatically picks up the latest model from S3.

Endpoints: `GET /health`, `GET /features` (dropdown options from the trained OHE categories), `POST /predict`.

### 3.5 Dashboard (data delivery)

**Container:** `accident-severity/dashboard`
**Code:** [dashboard/app/main.py](dashboard/app/main.py), [dashboard/static/](dashboard/static/)

FastAPI service on port 8000. Serves:
- `GET /` — single-page HTML application
- `GET /api/*` — REST endpoints reading from the 4 Postgres tables (kpi-geo, conditions, hotspots, vehicle-profile, top-weather, top-districts)
- `POST /api/predict` + `GET /api/predict/features` — reverse-proxy to `ml-predict:8001`
- `WS /ws` — WebSocket broadcaster: every 10 seconds a background coroutine builds a snapshot query (stats, recent geo, top districts, top weather, age bands) and pushes it as JSON to all connected clients

The dashboard is the only microservice exposed to end users; ml-predict is accessed only over the internal Docker network (`http://ml-predict:8001`).

---

## 4. Data Source

**Dataset:** [UK Road Safety Data — Accidents and Vehicles](https://www.kaggle.com/datasets/tsiaras/uk-road-safety-accidents-and-vehicles) (Kaggle)

| File | Rows | Time reference |
|------|------|----------------|
| `Accident_Information.csv` | ~2,047,256 | `Date` (YYYY-MM-DD) + `Time` (HH:MM) columns |
| `Vehicle_Information.csv` | ~2,058,408 | Joined via `Accident_Index` to accident timestamp (pre-2005 records removed) |

Both files far exceed the 1,000,000-data-point requirement.

**Download & prepare data:**

CSV files are excluded from git (too large). Use the provided script to download from Kaggle, sort by `Accident_Index`, and filter pre-2005 vehicle records:

```bash
# Install Kaggle CLI
pip install kaggle

# Set credentials (get from kaggle.com → Account → Settings → API)
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_api_key

# Download + prepare
bash download_data.sh
```

This script downloads, sorts both CSVs by `Accident_Index`, and removes 118,797 pre-2005 records from the vehicle file — ensuring both streams produce matching `Accident_Index` values from the start of replay.

**Real-time simulation:** The CSVs are static, so the producer replays them as a continuous event stream at a configurable rate (default 100 msg/s per stream, ~200 msg/s total). Each row is enriched with `ingest_time` (wall-clock UTC) before being pushed to Kafka. The Spark jobs read `startingOffsets=latest` and process the live stream — from Spark's perspective this is indistinguishable from a genuine IoT sensor feed. `MAX_RECORDS=0` (default) streams the entire dataset; setting it to a small number (e.g. 5000) enables a quick smoke-test run.

---

## 5. Reliability, Scalability & Maintainability

### Reliability

| Mechanism | Where applied |
|-----------|---------------|
| Kafka replication (Event Hubs manages internally) | Message durability — a broker failure does not lose messages |
| `acks=all`, `retries=5` in producer | No message is acknowledged until all replicas confirm receipt |
| Glue auto-restart on job failure (`timeout=2880` min) | Streaming jobs restart from last committed S3 checkpoint |
| S3 checkpointing | Exactly-once offset tracking for Spark Structured Streaming |
| `failOnDataLoss=false` | Stream continues when Kafka offset gaps appear (e.g. Event Hubs retention expiry) |
| READ-MERGE-WRITE UPSERT in Glue jobs | Postgres is the authoritative accumulated state; a full Glue restart re-processes and converges to the same totals |
| `restart: unless-stopped` in docker-compose.yml | Local containers restart automatically after crash |
| Docker healthchecks on all three containers | Compose waits for dependencies to be healthy before routing traffic |
| asyncpg connection pool (`min=2, max=10`) in dashboard | Survives transient RDS connection drops |

### Scalability

| Axis | Mechanism |
|------|-----------|
| Kafka partitions | 4 partitions per topic — producer partitions by `Accident_Index`; Glue jobs can scale workers to match |
| Glue workers | `glue_number_of_workers` variable — increase for higher throughput without code changes |
| Event Hubs Throughput Units | Terraform `eventhub_sku` / manual TU scaling handles burst ingestion |
| `maxOffsetsPerTrigger=10000` | Caps the per-batch size so a backlog spike does not crash Glue executors |
| Dashboard connection pool | asyncpg pool scales to 10 concurrent DB connections; increase `max_size` for higher WS concurrency |
| Producer `--rate` flag | Decouple ingestion speed from processing speed; back-pressure is absorbed by Kafka |

### Maintainability

- **IaC first:** every resource (VPC, S3, EC2, RDS, Glue jobs, Event Hubs, Secrets) is provisioned by Terraform; `terraform destroy` tears everything down cleanly.
- **Modular Terraform:** separate modules under `infra/modules/` for vpc, s3, ec2, rds, glue, eventhubs — each module has typed variables and outputs.
- **Shared Glue helpers:** `common.py` and `schemas.py` are uploaded once and referenced by all five jobs, avoiding code duplication.
- **Environment variables only:** no secrets are hardcoded — `.env.example` documents every variable.
- **Docker Compose local stack:** one `docker compose up -d` starts all three consumer-facing services; no manual dependency installation.
- **Version-controlled scripts:** Terraform uploads Glue scripts via `etag = filemd5(...)`, so a `terraform apply` after a script edit automatically re-deploys.

---

## 6. Data Security, Governance & Protection

| Control | Implementation |
|---------|----------------|
| Encryption in transit | Kafka: SASL_SSL (TLS 1.2+). S3: HTTPS only (bucket policy enforces `aws:SecureTransport`). RDS: SSL enforced at parameter group level. |
| Encryption at rest | S3: SSE-S3 (AES-256) enabled by bucket configuration. RDS: storage encryption via `storage_encrypted = true`. |
| Secrets management | Event Hubs connection string and Postgres credentials are stored in AWS Secrets Manager; Glue jobs retrieve them at runtime via `get_secret()` — never in environment variables or logs. |
| Least-privilege IAM | Glue role policy is scoped to the specific S3 bucket ARN and the two Secrets Manager ARNs. No wildcard resources on sensitive actions. |
| Network isolation | All resources sit inside a VPC (`10.0.0.0/16`). RDS security group allows port 5432 only from the Glue security group and optional `rds_extra_ingress_cidrs`. |
| No credentials in code | `KAFKA_SASL_PASSWORD` is set via Docker environment injection, never written to disk or committed. The `.gitignore` excludes `terraform.tfvars` and `.env`. |
| Data governance | Raw data is partitioned under `raw/` in S3; processed Parquet under `processed/`; model artifacts under `models/`. Glue job logs are sent to CloudWatch. S3 versioning can be enabled per module variable. |

---

## 7. Docker Images

| Service | Base image | Modifications |
|---------|-----------|---------------|
| `producer` | `python:3.11-slim` | Installs `kafka-python-ng`, `boto3` (S3 streaming + Secrets Manager) |
| `ml-predict` | `python:3.11-slim` | Installs `fastapi`, `uvicorn`, `scikit-learn`, `joblib`, `boto3` |
| `dashboard` | `python:3.11-slim` | Installs `fastapi`, `uvicorn`, `asyncpg`, `httpx`, serves static files |

All three use slim base images to minimise attack surface and image size. No root user in any container (standard python slim default).

AWS Glue Streaming jobs run on AWS-managed Spark containers (Glue version 4.0, Spark 3.3). Additional Python packages are specified via `--additional-python-modules` and `--extra-jars` (PostgreSQL JDBC driver) in Terraform.

---

## 8. Aggregation & Windowing

The system uses **stateless micro-batching via `foreachBatch`** rather than Spark's built-in stateful windowing operators. This choice is deliberate:

### Pattern: foreachBatch + READ-MERGE-WRITE UPSERT

```
Every 1 minute (processingTime trigger)
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 1. Spark collects all messages arriving in the 1-min window        │
  │ 2. foreachBatch aggregates the batch in Spark (groupBy + agg)      │
  │ 3. Collect aggregated rows to driver                               │
  │ 4. Read matching rows from Postgres (chunked IN-clause)            │
  │ 5. Merge in memory: merged_count = batch_count + existing_count    │
  │ 6. INSERT … ON CONFLICT DO UPDATE (UPSERT) merged rows to Postgres │
  └─────────────────────────────────────────────────────────────────────┘
```

Code location: [`infra/glue_scripts/common.py`](infra/glue_scripts/common.py) — `merge_then_upsert()`.

### Aggregation functions per job

| Job | Window type | Key aggregations |
|-----|-------------|-----------------|
| job1 | Daily (by `event_date`) | `COUNT(*)`, `SUM(fatal)`, `SUM(serious)`, `SUM(slight)`, `SUM(casualties)`, `SUM(vehicles)` grouped by 0.1° geo-grid cells |
| job2 | Daily | `COUNT(*)`, `SUM(fatal)`, `SUM(severity_int)` grouped by weather/light/road-surface/speed combinations |
| job3 | Daily | Weighted count (`fatal×3 + serious×2 + slight×1`) per district + road type |
| job4 | Annual (by `year`) | `COUNT(vehicles)`, `SUM(age_of_vehicle)` grouped by driver demographic + vehicle type |
| job5 | None (raw extract) | Selects `Accident_Index` + `Accident_Severity` from `accident-raw` → `processed/accidents/`; selects `Accident_Index` + driver/vehicle columns from `vehicles-raw` → `processed/vehicles/` |

### Why not Spark streaming windows?

Spark's `window()` / `watermark()` aggregation maintains in-executor state, which becomes invalid on checkpoint loss or job restart. The foreachBatch + Postgres pattern keeps state in the database, which survives Glue job failures and restarts without any manual state recovery.

---

## 9. Infrastructure as Code

All cloud infrastructure is managed by Terraform. No manual AWS Console or Azure Portal configuration is required after filling in `terraform.tfvars`.

**Modules:**

```
infra/
├── main.tf                  # wires modules together
├── variables.tf             # all tunable parameters
├── outputs.tf               # KAFKA_BROKERS, DB_HOST, etc.
├── modules/
│   ├── vpc/                 # VPC, 2 public subnets, IGW, route table
│   ├── s3/                  # S3 bucket + folder prefixes
│   ├── eventhubs/           # Azure Event Hubs namespace + 2 event hubs (topics)
│   ├── ec2/                 # t3.micro producer host, bootstrap script, IAM instance profile
│   ├── rds/                 # db.t3.micro Postgres, SG, Secrets Manager secret
│   └── glue/                # 5 streaming jobs + ML train job + Glue Trigger + IAM role
└── glue_scripts/            # PySpark job source uploaded to S3 on apply
```

**Deploy:**

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # fill in passwords + Azure sub ID
terraform init
terraform apply
```

Outputs include `eventhub_connection_string`, `rds_endpoint`, `ec2_public_ip` — these are piped into the EC2 bootstrap script and are also used to populate `.env` for local Docker Compose.

---

## 10. Local Quick-Start

Requirements: Docker Desktop, AWS credentials with S3 read access.

```bash
# 1. Download and prepare data
bash download_data.sh

# 2. Configure environment
cp .env.example .env          # fill in AWS creds, DB_HOST, DB_PASS, KAFKA_* values

# 3. Start services
docker compose up -d
open http://localhost:8000    # dashboard
# POST http://localhost:8001/predict for direct ML inference
```

The producer container streams CSVs from S3 into Event Hubs at the rate set by `PRODUCER_RATE`. Set `PRODUCER_MAX_RECORDS=5000` in `.env` for a quick smoke test.

To run producer only for a single stream:

```bash
docker compose run --rm producer python producer.py --only accidents --max-records 1000
```

---

## 11. Cloud Deployment

The producer is the only service deployed to a cloud VM. The Glue jobs and dashboard/ml-predict containers run in their respective managed environments.

```bash
cd infra && terraform apply   # provisions EC2, attaches bootstrap script
# Bootstrap script (infra/templates/bootstrap.sh.tftpl):
#   - installs Docker on EC2
#   - docker run accident-severity/producer with injected env vars
```

For the dashboard and ml-predict microservices in production, the Docker Compose file has an EC2 variant ([docker-compose.ec2.yml](docker-compose.ec2.yml)) that can be deployed directly on the same or a separate EC2 instance.

---

## 12. Project Reflection (Finalization Phase)

### Does the system fulfil the technical requirements?

Yes. The system continuously ingests two streams (>1M data points each, timestamped) via Kafka, processes them in near-real-time (1-minute micro-batches) with five Spark Structured Streaming jobs, accumulates aggregations in Postgres, retrains an ML model every 15 minutes, and delivers live updates to a browser dashboard via WebSocket. All infrastructure is defined in Terraform (IaC) and all services run in Docker containers.

### What went wrong and why?

1. **Azure Event Hubs SASL_SSL gotchas.** Three issues had to be resolved:
   - `systemd` silently expands `$ConnectionString` to empty in `EnvironmentFile=` — resolved by injecting the full connection string as `KAFKA_SASL_PASSWORD` and hardcoding the username as the literal string `"$ConnectionString"` in Python.
   - `kafka-python`'s default API version negotiation advertises Kafka 2.5+ features (idempotent producer, message format v2) that Event Hubs rejects — resolved by pinning `api_version=(2, 0, 0)`.
   - `compression_type='gzip'` was not supported by the Event Hubs Kafka surface at the Standard tier — removed.

2. **Job5 stream-stream join timing failure.** The original job5 attempted a Spark stream-stream join between `accident-raw` and `vehicles-raw` using a 10-minute processing-time watermark. The two CSVs start from different years (accidents from 2005, vehicles from 2004), so matching `Accident_Index` records arrive hours apart in wall-clock time — far outside the join window. Zero joined rows were ever produced. The fix was to remove the join entirely: job5 now extracts each stream independently to separate S3 Parquet folders (`processed/accidents/`, `processed/vehicles/`), and `train.py` performs the join in Pandas at training time.

3. **Glue VPC connection complexity.** Glue requires a self-referencing security group ingress rule for its VPC connection ENIs — this is undocumented and caused intermittent `Connection refused` errors to RDS until the self-reference rule was added in Terraform.

4. **Azure for Students region restrictions.** The Azure subscription is restricted to 5 regions; `eastus` was used as the default but requires explicit declaration in `terraform.tfvars` rather than relying on provider defaults.

### Is the system reliable, scalable, and maintainable?

**Reliable:** Yes, by design. Kafka retains messages for 24 hours (Event Hubs default retention); if a Glue job fails, it restarts from its S3 checkpoint and replays missed offsets. The Postgres UPSERT pattern ensures no double-counting on Glue restarts. Docker `restart: unless-stopped` handles container crashes locally.

**Scalable:** Horizontally for ingestion (add Kafka partitions + Glue workers). The dashboard scales via asyncpg pool and Uvicorn workers. The ML training job is stateless and can be parallelised by dataset partition if needed.

**Maintainable:** IaC-first design means the entire stack can be torn down and recreated with `terraform destroy && terraform apply`. Adding a new aggregation dimension requires writing one new Glue script and one new Terraform `for_each` entry — no changes to other services.

### What security measures could be added?

- **Column-level encryption** for any PII columns in Postgres (not present in this dataset, but relevant if real accident reports include personal data).
- **VPC endpoints for S3 and Secrets Manager** to prevent traffic leaving the VPC over the internet.
- **IAM condition keys** to restrict S3 bucket access to specific Glue job ARNs only.
- **Kafka consumer group ACLs** on Event Hubs via SAS policy scoping (currently one connection string has full namespace access).
- **WAF** in front of the dashboard if exposed publicly.
- **Audit logging** via AWS CloudTrail for Secrets Manager access and S3 object-level events.

### What could be improved in the next project?

- Use **Confluent Kafka** (librdkafka binding) instead of `kafka-python` to unlock the idempotent producer and exactly-once semantics end-to-end.
- Replace the foreachBatch UPSERT with **Apache Hudi or Delta Lake** on S3 for proper exactly-once stream-to-lake semantics and time-travel queries.
- Add **schema registry** (Confluent Schema Registry or AWS Glue Schema Registry) to enforce the producer envelope schema and catch breaking changes at publish time.
- **Parameterise the dashboard polling interval** and add a proper message queue (e.g. Redis Pub/Sub) between Glue and the dashboard instead of Postgres polling.

### Three most valuable technical skills learned

1. **Apache Spark Structured Streaming** — understanding the foreachBatch pattern, checkpoint semantics, and how to avoid stateful operator pitfalls in a managed Glue environment.
2. **Terraform multi-provider IaC** — managing cross-cloud resources (AWS + Azure) in one `terraform apply`, using modules, remote state, and template files for bootstrapping.
3. **Kafka producer tuning for Azure Event Hubs** — diagnosing and resolving SASL_SSL authentication, API version pinning, and the `$ConnectionString` literal username requirement specific to the Event Hubs Kafka surface.

### Three most valuable soft skills learned

1. **Incremental problem decomposition** — breaking a complex streaming pipeline into independently testable layers (produce → broker → process → store → serve) and validating each before wiring them together.
2. **Documentation as a first-class deliverable** — keeping Terraform variables, `.env.example`, and code comments aligned with the actual implementation so the system is reproducible by someone else without verbal explanation.
3. **Tolerating ambiguity in distributed systems** — accepting that at-least-once delivery is the default, designing for idempotent consumers, and choosing simplicity (UPSERT) over theoretical perfection (exactly-once Kafka transactions) given the prototype constraints.

### Strategy for introducing a batch pipeline

The natural extension is a **Lambda Architecture** layer on top of the existing speed layer:

1. **Batch ingestion:** Schedule a daily AWS Glue ETL job (not streaming) that reads the full `raw/` S3 prefix, applies heavy transformations (deduplication, geospatial enrichment, feature engineering), and writes a corrected Parquet dataset to `processed/batch/`.
2. **Serving layer merge:** Add a second set of Postgres tables (`accident_kpi_geo_batch`, etc.) populated by the batch job with `ON CONFLICT DO UPDATE` semantics identical to the streaming jobs. The dashboard can query both — streaming for recency, batch for historical accuracy.
3. **Trigger:** Use an S3 Event Notification → Lambda → Glue Job trigger, or simply an AWS EventBridge scheduled rule, to run the batch job nightly.
4. **No code reuse conflict:** The five Glue streaming jobs are already pure Spark; the batch versions would reuse `schemas.py` and switch from `readStream` to `spark.read` (batch mode), requiring minimal changes.

This avoids touching the real-time path and adds historical correctness without downtime.
