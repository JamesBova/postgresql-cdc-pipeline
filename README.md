# PostgreSQL Change Data Capture Pipeline

An end-to-end Change Data Capture (CDC) pipeline that replicates changes from a PostgreSQL source database to a PostgreSQL destination using Debezium, Apache Kafka, and Python.

## Architecture

PostgreSQL Source
    |
    | Write-Ahead Log (WAL)
    v
Debezium / Kafka Connect
    |
    v
Apache Kafka
    |
    | CDC Events
    v
Python Consumer
    |
    v
PostgreSQL Target

## Technologies

- PostgreSQL 16
- Apache Kafka 3.9.1
- Debezium 3.3
- Python
- confluent-kafka
- psycopg
- Docker Compose

## How It Works

1. PostgreSQL records database changes in its WAL.
2. Debezium reads changes through logical replication.
3. Debezium publishes CDC events to Kafka.
4. A Python consumer reads the events.
5. The consumer applies corresponding changes to the destination database.
6. Kafka offsets are committed after successful database transactions.

## Supported Operations

| Operation | CDC Code | Destination Behavior |
|-----------|----------|----------------------|
| Snapshot | r | Insert or update |
| Insert | c | Insert or update |
| Update | u | Insert or update |
| Delete | d | Delete record |

The consumer uses PostgreSQL ON CONFLICT to handle duplicate inserts and updates safely.

Delete events use the primary key from the before record.

Kafka tombstone records are skipped.

## Data Types

Debezium encodes PostgreSQL NUMERIC values as Base64 representations of unscaled decimal values.

The consumer decodes these values into Python Decimal objects while preserving the original scale.

## Reliability

- Manual Kafka offset commits
- Database transactions
- Idempotent upserts and deletes
- At-least-once processing semantics
- Persistent Docker volumes

The implementation is intended for local development and portfolio demonstration, not production deployment.

## Project Structure

config/
    postgres-connector.json

sql/
    create_customers.sql
    seed_source.sql

src/
    consumer.py

docker-compose.yml
requirements.txt
.env.example

## Setup

Requirements:

- Docker Desktop
- Python 3
- PowerShell (for the commands below)

Create the local environment file:

    Copy-Item .env.example .env

Start the containers:

    docker compose up -d

Create the customers table in both databases:

    Get-Content sql\create_customers.sql | docker compose exec -T postgres-source psql -U cdc_user -d source_db

    Get-Content sql\create_customers.sql | docker compose exec -T postgres-target psql -U cdc_user -d target_db

Seed the source database (run once):

    Get-Content sql\seed_source.sql | docker compose exec -T postgres-source psql -U cdc_user -d source_db

Register the Debezium connector:

    Invoke-RestMethod -Uri "http://127.0.0.1:8083/connectors" -Method Post -ContentType "application/json" -InFile "config\postgres-connector.json"

Create and activate a Python environment:

    python -m venv .venv
    .\.venv\Scripts\Activate.ps1

Install dependencies:

    python -m pip install -r requirements.txt

Start the consumer:

    python src\consumer.py

## Testing

Insert, update, or delete a customer in the source database.

The consumer processes each corresponding CDC event and applies the change to the destination database.

Verify the results by querying the customers table in the target database.

## Notes

This project demonstrates database change capture, event streaming, transactional processing, and idempotent data replication.

It uses a single Kafka broker and is designed for local development.
