FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy Python scripts and SQL query
COPY data_pipeline.py .
COPY query.sql .

# Copy data directory into the container
COPY data/ /app/data/

CMD ["python", "data_pipeline.py"]