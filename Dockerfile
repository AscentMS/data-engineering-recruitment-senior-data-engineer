# Use Python 3.12 slim base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy project files
COPY pyproject.toml poetry.lock* README.md /app/
COPY src/ /app/src/

# Install dependencies
RUN poetry config virtualenvs.create false && poetry install --only main

# Run the pipeline
ENTRYPOINT ["python", "src/pipeline.py"]
