FROM python:3.9-slim

# Install dependencies
RUN apt-get update && \
    apt-get install -y gcc libpq-dev

# Install Python packages
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow

# Install Airflow
RUN pip install apache-airflow

# Copy the scripts and dags
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY setup/ /opt/airflow/setup/

# Copy entrypoint script
COPY setup/start_airflow.sh /opt/airflow/start_airflow.sh
RUN chmod +x /opt/airflow/start_airflow.sh

# Expose Airflow webserver port
EXPOSE 8080

CMD ["/opt/airflow/start_airflow.sh"]
