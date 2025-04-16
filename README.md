# Senior Data Engineer Interview Task 
At Evergreen we deal with disparate sources of sensitive customer and geographical data across various departments, with examples ranging over structured, to semi-structured, to unstructured data. Our objective is to centralise these data sources into our relational data warehouse to extract valuable insights and facilitate data-driven decisions, whilst respecting the uniquely sensitive nature of health data.

The task below touches on several different areas of how data engineering operates at Evergreen, spanning data modelling, integration, deployment, etc. We ask that you try to complete as much as you can within **120 minutes**. The task methodology is purposefully open to allow us to observe your approach to the problem: our only prerequisite is that you use Python as your programming language, and demonstrate some flavour of SQL somewhere in the solution.

## Task
Your task is to develop a data pipeline that retrieves and integrates two datasets into an analytics database to query for specific information. Write a Python script which retrieves the two files below, and loads them into a local relational database (an in-memory / file base database is fine). Then, using your database, write a SQL query that demonstrates how many distinct users there are by local authority. Create a fork of this repository to hold your code.

- The Customer Data file should be anonymised prior to being used for analytics.
- Account for potential changes in the data, such as when a user moves home.
- The data must be formatted such that it allows a data scientist/analyst to easily determine how many users there are in any given local authority

Create a branch in your forked version of the repository to hold your solution, and commit all your changes to the branch. When you're finished, submit a Pull Request: we will then review the solution in the follow-up interview. Feel free to add comments both in the code itself and in the Pull Request to clarify on any points you feel necessary.

To make it easier for others to run your code and demonstrate code deployment principles, please add your solution to a container image.

Do not commit the processed data / credentials / etc. to source control; we expect you to demonstrate the process executing.
## Data Sources
- [Postcode Data](https://evergreen-life-interview.s3.eu-west-2.amazonaws.com/postcodes.zip)
- [Customer Data](https://evergreen-life-interview.s3.eu-west-2.amazonaws.com/users.json)

## Bonus Considerations 
- Package your code as a python package.
- Show how your code could be utilised in an Airflow DAG.
- Demonstrate other analyses / metrics that could be retrieved from these datasets.
- Add tests to your package.
- Demonstrate good database practices as appropriate to the technology (e.g. indexing, row vs columnular orientation, etc.)
- Demonstrate data / software engineering principles: e.g. Idempotency, SOLID design, etc.

## Data Pipeline documentation
A data pipeline for integrating customer and postcode data.

### Setup
1. Install Poetry: `pip install poetry`
2. Install dependencies: `poetry install`
3. Run the pipeline: `poetry run python src/pipeline.py`
4. Build Docker image: `docker build -t data-pipeline .`
5. Download and put the datasets inside the data folder, see README.md on data/ for more info.

## Development Considerations

### Database Choice
SQLite was chosen for its simplicity, portability, and zero-configuration setup, ideal for this exercise with static datasets (`Customer_Data.json`, `National_Statistics_Postcode_Lookup_Latest_Centroids.csv`). The database resides at `analytics.db` in the project root (moved from `data/` for visibility). In production, I’d opt for PostgreSQL or MySQL for better concurrency and scalability.

# Data Pipeline for Customer and Postcode Data

A robust data pipeline that fetches customer and postcode data from remote endpoints, anonymizes customer data, stores it in an SQLite database, and performs analytical queries. Built with Python 3.12, Pandas, SQLite, and containerized with Docker. Designed for simplicity, reliability, and idempotency.

## Features
- **Dynamic Data Fetching**: Downloads customer (JSON) and postcode (CSV) data from URLs, handling ZIP extraction and large files with streaming.
- **Data Anonymization**: Hashes customer emails with SHA-256, standardizes postcodes, and validates sex fields.
- **Data Quality**: Logs invalid records to `invalid_records.json` for traceability.
- **Database**: Stores data in SQLite with indexes on `postcode` for fast queries.
- **Analytics**: Queries users by local authority and sex.
- **Idempotency**: Ensures consistent results with table replacement in database operations.
- **Containerization**: Runs in a lightweight Docker container.
- **Orchestration**: Compatible with Apache Airflow for scheduling.

## Prerequisites
- Python 3.12
- Docker (optional, for containerized execution)
- Poetry (for dependency management)
- GitHub (optional, for CI/CD with GitHub Actions)

## Setup
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd data-engineering-recruitment-se
2. **Install Poetry**:
   ```bash
   pip install poetry
3. **Install Dependencies**:
   ```bash
   poetry install --only main
4. **Run the Pipeline**:
   ```bash
   poetry run python src/pipeline.py <customer-data-url> <postcode-data-url>
   Replace <customer-data-url> and <postcode-data-url> with the respective data endpoints.
5. **Build and Run with Docker**:
   ```bash
   docker build -t data-pipeline .
   docker run --rm data-pipeline <customer-data-url> <postcode-data-url>

## Development
- Run Tests:
  ```bash
  poetry install
  poetry run pytest tests/

## Pipeline Workflow
- Fetch Data: Downloads customer (JSON) and postcode (CSV) data from URLs, extracting ZIPs if needed using streaming to handle large files.
- Anonymize Customer Data: Parses JSON with a brace-counting approach, validates emails, sexes, and postcodes, hashes emails, and logs invalid records.
- Process Postcode Data: Standardizes postcodes and maps to local authorities from CSV data.
- Store Data: Loads data into SQLite tables (customers, postcodes) with indexes for performance.
- Analyze Data: Runs SQL queries to count users by local authority and sex.

## Design Decisions
- SQLite: Chosen for simplicity, portability, and zero-configuration setup. Suitable for this scope, but PostgreSQL/MySQL would be considered for production scalability.
- Dynamic Data Fetching: Uses requests with chunked streaming to handle large files efficiently, replacing static datasets to avoid versioning large files in Git.
- JSON Parsing: Employs a brace-counting parser to handle multi-line JSON objects reliably, with error logging for invalid records.
- Anonymization: Emails are hashed with SHA-256 for privacy; postcodes are standardized with regex validation; sex is normalized to M/F.
- Idempotency: Database tables are replaced each run (if_exists='replace') to ensure consistent results, a paramount requirement.
- Error Handling: Comprehensive logging and invalid record tracking ensure data quality and debuggability.
Indexes: Added on postcode columns to optimize SQL joins and queries.

## Engineering Principles
- Idempotency: Critical for reliability; achieved through table replacement in database operations.
- Modularity: Single-responsibility functions (e.g., anonymize_customer_data, process_postcode_data) for maintainability.
- Data Quality: Validation and logging ensure reliable outputs.
- Simplicity: Avoided complex libraries and overhead to minimize bugs and meet deadlines.
- Scalability: Streaming and chunked processing handle large datasets efficiently.

## Considerations for Future Improvements
To keep the pipeline simple, avoid introducing bugs, and meet time constraints, several enhancements Ire considered but not implemented. These are planned for future iterations:

- Streaming JSON Parsing with ijson: I evaluated ijson for incremental JSON parsing to handle very large files more efficiently. HoIver, prior attempts revealed compatibility issues, so I retained the reliable brace-counting parser to ensure stability and simplicity.
- Configurable Invalid Records Path: I considered making the invalid_records.json output path configurable (e.g., via CLI or config file) to improve flexibility, but kept it hardcoded to avoid additional complexity.
- Parallel Processing: I explored using multiprocessing or concurrent.futures to parallelize customer data validation and anonymization for performance gains on large datasets. This was deferred to avoid overhead and potential bugs.
- Consolidated Logging: I wanted to consolidate logging to report summary statistics (e.g., valid/invalid record counts) once at the end of processing, but retained per-step logging for immediate debuggability.
- Expanded Test Coverage: I planned to add unit tests for fetch_data_from_url, process_postcode_data, and database queries, using pytest-mock to simulate HTTP requests. Current tests cover critical functions, and expansion was deferred to prioritize delivery.
- Observability: I considered adding Prometheus metrics (via prometheus_client) to track processing time and record counts, and structured logging with structlog for integration with tools like ELK or Prometheus. These Ire skipped to avoid dependency overhead and complexity.
- Docstrings: I wanted to add docstrings to all functions (e.g., fetch_data_from_url) for better code documentation, but omitted them to focus on functionality.
- Incremental Database Updates: While the current if_exists='replace' ensures idempotency, we considered INSERT OR REPLACE or UPSERT for incremental updates to support partial data refreshes, deferred for simplicity.
- Dynamic Configuration: We explored using environment variables or a config file (e.g., with pydantic) for URLs, database paths, and logging levels, but used CLI args to keep the setup straightforward.