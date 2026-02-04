FROM apache/airflow:2.7.3-python3.11

# Copy your DAGs
COPY ./dags /opt/airflow/dags

# Copy any requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

