# Paris Vélib Availability Pipeline

An end-to-end batch data engineering project that analyses Paris Vélib station availability and identifies stations that may require bicycle rebalancing.


## Problem Statement

Vélib users may arrive at a station and find no bicycle available to rent or no free dock to return a bicycle.

This project builds an automated batch data pipeline to process historical Vélib station snapshots and weather observations. The final dashboard will help identify:

- Stations that are frequently empty or full
- Hours and days with the greatest availability problems
- Differences between mechanical and electric bicycle availability
- Possible relationships between weather and bicycle availability
- Stations that may require frequent rebalancing

## Dataset

The project uses the [Vélib Data dataset from Kaggle](https://www.kaggle.com/datasets/adrienmorel97/velib-data).

Each row represents the condition of one Vélib station during a five-minute time interval.

The dataset contains:

- Snapshot timestamps
- Station identifiers and names
- Station coordinates
- Available mechanical bicycles
- Available electric bicycles
- Station capacity and operational status
- Temperature, precipitation and wind speed

The dataset contains station snapshots rather than individual bicycle journeys. Therefore, this project analyses availability patterns and does not attempt to reconstruct exact trips.

## Architecture

The data flow is:

1. Ingest the source Parquet data using Python
2. Store the raw data in Google Cloud Storage
3. Load the data into BigQuery
4. Clean and transform the data using dbt
5. Create analytical models for station reliability and availability
6. Visualize the results in Looker Studio

## Technologies

- Python
- Docker
- Google Cloud Platform
- Google Cloud Storage
- BigQuery
- Terraform
- Kestra
- dbt
- Looker Studio
- GitHub Actions

## Dashboard

The dashboard include:

- Bicycle availability over time
- Mechanical versus electric bicycle distribution
- Stations frequently empty or full
- Rebalancing-priority stations
- Availability patterns under different weather conditions

## Repository Structure

```text
.
├── dashboard/       # Dashboard documentation and screenshots
├── data/raw/        # Local raw data (excluded from Git)
├── dbt/             # dbt transformations and tests
├── docs/            # Architecture and project documentation
├── ingestion/       # Python ingestion pipeline
├── orchestration/   # Kestra workflows
├── terraform/       # GCP infrastructure as code
└── tests/           # Python tests