FROM apache/airflow:2.8.0-python3.12

# Copy your DAGs
COPY ./dags /opt/airflow/dags

# Copy any requirements
COPY requirements.txt .
RUN pip install -r requirements.txt
