# AWS Trade Compliance Data Lake Pipeline

## Project Overview

This project simulates a **production-style data engineering pipeline** for processing international trade compliance data.

The pipeline demonstrates how raw CSV datasets can be validated, transformed, and stored in a **cloud-based data lake architecture using AWS services**.

This repository includes:

•  Python-based ETL framework
•  AWS Data Lake pipeline using Glue and Athena

---

# Architecture

## Python ETL Framework

```text
Raw CSV Data
      ↓
Validation Layer
      ↓
Transformation (Pandas)
      ↓
Parquet Conversion
      ↓
Processed Data Layer
      ↓
Metadata & Logging
```

### Features

* Schema validation
* Incremental ingestion
* Partitioned Parquet storage
* Logging and metadata tracking

### Technologies

* Python
* Pandas
* Parquet
* Logging

---

### AWS Data Lake Pipeline

```text
CSV File
   ↓
Amazon S3 (Raw Layer)
   ↓
AWS Glue Crawler
   ↓
AWS Glue Spark ETL Job
   ↓
Parquet Output
   ↓
Amazon S3 (Processed Layer)
   ↓
Amazon Athena SQL Queries
```

### Features

* Data lake architecture
* Serverless Spark ETL processing
* Cloud-based storage using S3
* SQL analytics using Athena

### Technologies

* AWS S3
* AWS Glue
* Apache Spark
* AWS Athena
* IAM
* CloudWatch Logs

---

# Data Lake Structure

```
trade-compliance-data-lake
│
├── raw/
│     └── imports/
│          └── imports.csv
│
├── processed/
│     └── imports/
│          └── parquet files
│
└── athena-results/
```

---

# Example Dataset

| trade_id | product        | country      | import_value | date       |
| -------- | -------------- | ------------ | ------------ | ---------- |
| 1        | Semiconductor  | China        | 500000       | 2024-01-10 |
| 2        | Pharmaceutical | Germany      | 200000       | 2024-01-12 |
| 3        | Steel          | India        | 300000       | 2024-01-15 |
| 4        | Oil            | Saudi Arabia | 800000       | 2024-01-18 |

---

# Key Learning Outcomes

This project demonstrates:

* Building modular ETL pipelines
* Implementing data validation layers
* Designing cloud data lake architectures
* Performing Spark-based data transformations
* Querying S3 data using Athena
* Debugging pipelines with CloudWatch logs

---

# Future Improvements

Potential enhancements:

* Automated pipeline triggers using S3 events
* Lambda-based orchestration
* Glue job scheduling
* Data quality monitoring

---

# Author

Gayathri Vaddepalli
Data Engineering | AI/ML | Cloud Platforms

