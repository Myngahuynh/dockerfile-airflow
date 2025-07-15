# Filename: Dockerfile
# Use the official Apache Airflow image. No extra system packages are needed for this basic setup.
FROM apache/airflow:2.8.1

# This section is kept for future dependencies.
# For now, it will just run pip install on an empty file.
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt