# Automated EOD Securities Pricing Analytics Platform

*(Repository: eod-market-data-engineering-platform)*

## Overview

This project implements a fully automated, end-to-end data engineering platform for processing U.S. End-of-Day (EOD) securities pricing and liquidity data.  
It replaces manual reporting workflows with a scalable Snowflake-based warehouse and Airflow-orchestrated batch processing.

### Core Componen

- Historical data backfill ingestion  
- Incremental daily batch processing  
- Multi-layer Snowflake data warehouse  
- Data validation and reject handling framework  
- Apache Airflow workflow orchestration  
- Slack-based failure alert notifications  
- BI-ready Subject Area (SA) publishing  

---

## Architecture

The warehouse follows a structured layered design:

**RAW → CORE → DIM → FACT → SA**

- **RAW** – Immutable ingestion layer  
- **CORE** – Cleansed and standardized datasets  
- **DIM** – Dimension tables for analytics joins  
- **FACT** – Transactional pricing data  
- **SA** – Curated, analytics-ready views  

---

## Key Features

- Incremental **MERGE-based loading strategy** in Snowflake  
- Pre-merge and post-merge data validation checks  
- Reject table handling for malformed or inconsistent records  
- Fully automated Airflow DAG scheduling  
- Slack notifications for pipeline monitoring and failure alerts  

---

## Pipeline Capabilities

- Processes EOD pricing for **6,000+ U.S. securities**  
- Automated historical backfill setup  
- Reliable daily incremental refresh before market open  
- Data quality enforcement at multiple pipeline stages  
- BI-ready curated datasets optimized for analytics consumption  