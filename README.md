# Agricultural Monitoring Data Pipeline

**[Click to view working video](https://drive.google.com/file/d/1abcXYZ789def/view?usp=sharing)**

## How to Run

This project has been dockerized.
To run simply clone this and run below commands.
1. **Ingestion Pipeline:**
    ```
    docker-compose run --build pipeline python main.py --action ingest
    ```
2. **Inspect a file:** 
    ```
    docker-compose run --build pipeline python main.py --action inspect --file data/raw/2023-06-01.parquet
    ```
3. **Run Tests:**
    1. **Run all the tests:**
        ```
        docker-compose run --build tests
        ```
    2. **Check test coverage:**
        ```
        docker-compose run --build test_coverage
        ```

> Note: Delete or empty the ingestion_checkpoint.json to clear the checkpoint and run the pipeline again

---
## Features Summary
1. **Ingestion**
    - Validates file path, schema, and types
    - Extracts dates from filenames
    - Tracks processed files to avoid duplication
    - Uses DuckDB to load Parquet files

2. **Transformation**
    - Deduplication & cleaning
    - Outlier removal using Z-score
    - Missing value handling
    - Normalization via calibration parameters
    - Derived fields:
        - Daily average
        - 7-day rolling average
        - Anomaly detection based on thresholds

3. **Validation (DuckDB)**
    - Generates data_quality_report.csv with:
        - Type and schema checks
        - Range validations
        - Missing hourly intervals (via generate_series)
        - Profiles:
            - % missing by reading_type
            - % anomalies
            - Time coverage gaps

4. **Storage**
    - Saves partitioned Parquet files under data/processed/
    - Partitioned by:
        - date (derived from timestamp)
        - sensor_id (optional)
    - Compression: snappy

---
## Scope for Improvement
While this assignment covers the core objectives, several aspects can be enhanced further for production-readiness:
    
- **Persistent & Cloud Logging**  

    Integrate structured logging (e.g., JSON) with support for centralized log systems like Stackdriver, CloudWatch, or ELK stack.

- **Retry & Failure Handling**  

    Add retry mechanisms for transient failures during ingestion, transformation, or storage stages.

- **Database Integration**  

    Replace flat file output with a persistent database (e.g., PostgreSQL, BigQuery) to support querying, indexing, and long-term storage.

> **Note:**  
> Due to time constraints and the scope of the assignment, several aspects had to be implemented in a simplified manner. The focus was on delivering a clear, functional, and testable pipeline within the limited timeframe provided.