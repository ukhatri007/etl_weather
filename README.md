# PostgreSQL Utility Package

A lightweight PostgreSQL utility package built using Python dataclasses and SQLAlchemy for database connections, querying, loading data, and managing tables.

## Features

* Create PostgreSQL database connections
* Execute SQL queries and fetch results into Pandas DataFrames
* Load Pandas DataFrames into PostgreSQL tables
* Check whether tables exist
* Delete tables dynamically
* Organized using dataclasses for cleaner code structure

---

# Project Structure

```text
project/
│
├── postgres_utils.py
├── README.md
└── requirements.txt
```

---

# Installation

Install required dependencies:

```bash
pip install pandas sqlalchemy psycopg2-binary
```

or using uv:

```bash
uv pip install pandas sqlalchemy psycopg2-binary
```

---

# Classes Overview

## 1. PostgresConnection

Responsible for creating a PostgreSQL database connection.

### Parameters

| Parameter | Type | Description         |
| --------- | ---- | ------------------- |
| database  | str  | Database name       |
| user      | str  | PostgreSQL username |
| password  | str  | PostgreSQL password |

### Example

```python
from postgres_utils import PostgresConnection

conn = PostgresConnection(
    database="weather_db",
    user="postgres",
    password="password"
)

engine = conn.create_conn()
```

---

## 2. PostgresOperation

Handles database operations such as:

* Checking table existence
* Dropping tables

### Example

```python
from postgres_utils import PostgresOperation

pg_ops = PostgresOperation(
    database="weather_db",
    user="postgres",
    password="password"
)

exists = pg_ops.check_if_table_exists(
    table_name="weather_data",
    schema_name="weather_schema"
)

print(exists)

pg_ops.delete_table(
    table_name="weather_data",
    schema_name="weather_schema"
)
```

---

## 3. PostgresDestination

Loads Pandas DataFrames into PostgreSQL.

### Example

```python
import pandas as pd
from postgres_utils import PostgresDestination

df = pd.DataFrame({
    "city": ["Kathmandu"],
    "temp": [28]
})

destination = PostgresDestination(
    database="weather_db",
    user="postgres",
    password="password"
)

destination.load_to_table(
    df=df,
    table_name="weather_data",
    if_exists="append"
)
```

---

## 4. PostgresSource

Fetches data from PostgreSQL into Pandas DataFrames.

### Example

```python
from postgres_utils import PostgresSource

source = PostgresSource(
    database="weather_db",
    user="postgres",
    password="password"
)

df = source.query_postgres(
    "SELECT * FROM weather_schema.weather_data"
)

print(df.head())
```

---

# Database Configuration

Current connection configuration:

```text
Host: localhost
Port: 5432
Database: Your Database
User: Your User
```

Connection string format:

```python
postgresql://USER:PASSWORD@localhost:5432/DATABASE
```

---

# Dependencies

```text
pandas
sqlalchemy
psycopg2-binary
dataclasses
```

---

# Future Improvements

* Add environment variable support
* Add logging
* Add exception handling
* Add configurable host and port
* Add connection pooling
* Add unit tests

---

# Notes

* `load_to_table()` currently loads into:

```text
weather_schema
```

* Ensure the schema exists before loading data.

* `delete_table()` permanently removes tables.

---

# License

MIT License
