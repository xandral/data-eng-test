# Common Crawl Data Processing Pipeline

This project sets up an automated pipeline for downloading, processing, and updating Common Crawl data using Apache Airflow, PostgreSQL, and Docker.

## Prerequisites

- Docker
- Docker Compose

## Setup

1. **Clone the repository:**

    ```sh
    git clone https://github.com/your-repo/common-crawl-pipeline.git
    cd common-crawl-pipeline
    ```

2. **Build the Docker image:**

    ```sh
    docker-compose build
    ```

3. **Setup PostgreSQL:**

    ```sh
    ./setup/setup_postgres.sh
    ```

4. **Setup Airflow:**

    ```sh
    ./setup/setup_airflow.sh
    ```

5. **Download Common Crawl segments:**

    ```sh
    ./setup/download_segments.sh
    ```

6. **Start Airflow:**

    ```sh
    ./setup/start_airflow.sh
    ```

7. **Teardown the environment (optional):**

    ```sh
    ./setup/teardown.sh
    ```

## Project Structure

- **dags/**: Contains the Airflow DAG definitions.
- **scripts/**: Contains Python scripts for downloading, loading, and updating data.
- **setup/**: Contains shell scripts for setting up and tearing down the environment.
- **docker-compose.yml**: Docker Compose configuration.
- **Dockerfile**: Docker image configuration.
- **requirements.txt**: Python dependencies.

## Airflow Web UI

Access the Airflow web UI at `http://localhost:8080` and log in with:

- **Username**: admin
- **Password**: admin

## Running the Pipeline

The Airflow pipeline (`extract_and_load_dag`) will download data, load it into PostgreSQL, and update the required fields.

1. Trigger the DAG from the Airflow web UI or run `./trigger_dag.sh <commoncrawl_url>`
2. To export postgres table run:
    1. python3 -m venv venv
    2. pip3 install -r requirements.txt
    3. source venv/bin/activate
    4. python table_export.py
  
3. To compote metrics follow the above statements to install th eenvironment and run python metrics/compute_metrics.py




