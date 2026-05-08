# E-Commerce Behavioral Analytics & Recommendation Engine

Big Data Engineering Project — Egypt University of Informatics, Spring 2026

## Overview

An end-to-end big data pipeline processing **10.8M e-commerce events** (~2.14 GB) using Apache Spark (MapReduce paradigm), MongoDB storage, and a Spark-MongoDB enrichment layer to generate a targeted marketing campaign.

## Tech Stack

| Component | Technology |
|---|---|
| **Processing** | Apache Spark 4.1.1 (PySpark) |
| **Storage** | MongoDB 8 |
| **Orchestration** | Docker |
| **Paradigm** | MapReduce |

## Dataset

- **File:** `Dataset/ecommerce_logs.csv` (2.14 GB, 10,831,817 rows)
- **Columns:** `timestamp`, `session_id`, `user_id`, `event_type` (view/cart/purchase), `product_id`, `price`, `referrer`, `user_metadata`, `product_metadata`

## Pipeline

```
Raw CSV ──→ Phase 1 (Spark MapReduce) ──→ MongoDB ──→ Phase 3 (Spark + Mongo) ──→ Campaign CSV
```

### Phase 1 — Distributed Processing
- **Task 1.1:** Market Basket Analysis — finds item pairs purchased together (65,672 pairs)
- **Task 1.2:** User Affinity Aggregation — builds per-user category profiles with weighted scores (view=1, cart=3, purchase=5) for 25,000 users

### Phase 2 — NoSQL Storage
- MongoDB document schema with denormalized user profiles
- Two collections: `user_profiles` (25K docs) and `item_pairs` (65,672 docs)

### Phase 3 — Enrichment Pipeline
- Identifies cart-abandonment sessions (cart with no purchase)
- Joins abandoned items with user profiles from MongoDB
- Flags each as `High_Discount` or `Standard_Reminder`
- Output: 1,442,694 campaign targets

## How to Run

### 1. Start Containers
```powershell
# Option A: double-click start.bat
# Option B:
docker compose up -d
```

### 2. Run the Pipeline
```powershell
# Option A: double-click run_all.ps1
# Option B:
.\run_all.ps1

# Skip completed phases:
.\run_all.ps1 -SkipPhase1 -SkipPhase2
```

### 3. Stop Containers
```powershell
# Option A: double-click stop.bat
# Option B:
docker compose down
```

### Run Individual Steps
```powershell
# Phase 1.1 — Market Basket Analysis
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] /opt/spark/scripts/phase1_market_basket.py

# Phase 1.2 — User Affinity
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] /opt/spark/scripts/phase1.2_user_affinity.py

# Phase 2 — MongoDB Ingestion
docker compose exec spark-master python3 /opt/spark/scripts/phase2_mongodb_ingest.py

# Phase 3 — Cart Abandonment Recovery
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] --driver-memory 4g /opt/spark/scripts/phase3_cart_abandonment.py

# Query Demo
docker compose exec spark-master python3 /opt/spark/scripts/query_demo.py --user User_42
```

## Project Structure

```
Ecom-bigdata-2/
├── Dataset/                          # Ignored by git (2.14 GB CSV)
├── output/                           # Pipeline outputs
│   ├── market_basket/                # Phase 1.1 — item co-occurrence pairs
│   ├── user_affinity/                # Phase 1.2 — user profiles (JSON)
│   └── campaign/                     # Phase 3 — marketing campaign targets
├── scripts/                          # All pipeline scripts
│   ├── phase1_market_basket.py       # Task 1.1
│   ├── phase1.2_user_affinity.py     # Task 1.2
│   ├── phase2_mongodb_ingest.py      # Tasks 2.1 & 2.2
│   ├── phase3_cart_abandonment.py    # Task 3.1
│   ├── query_demo.py                 # Deliverable 3 — MongoDB lookup demo
│   └── schema_design.md              # Deliverable 2 — NoSQL schema doc
├── start.bat                         # docker compose up
├── stop.bat                          # docker compose down
├── run_all.ps1                       # Run full pipeline via Docker
├── docker-compose.yml                # Spark master + worker + MongoDB
├── .gitignore
└── README.md
```

## Deliverables

| # | Deliverable | File |
|---|---|---|
| 1 | Source Code | `scripts/phase1_*.py`, `scripts/phase3_*.py` |
| 2 | Architecture & Schema Document | `scripts/schema_design.md` |
| 3 | Query Demo Script | `scripts/query_demo.py` |

## Outputs

| Output | Records | Location |
|---|---|---|
| Item co-occurrence pairs | 65,672 | `output/market_basket/` |
| User affinity profiles | 25,000 | `output/user_affinity/` |
| Campaign targets | 1,442,694 | `output/campaign/` |
