import sys
import json
import hashlib
import pandas as pd
import sqlite3
import logging
import re
from datetime import datetime
from src.data_fetcher import fetch_data_from_url

# Log to console only, no file complications
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Standardize postcode
def standardize_postcode(postcode):
    if not postcode or not isinstance(postcode, str):
        return None
    postcode = postcode.strip().replace(" ", "").upper()
    if len(postcode) < 3 or len(postcode) > 7:
        return None
    if not re.match(r'^[A-Z0-9]{3,7}$', postcode):
        return None
    return postcode

# Anonymize customer data
def anonymize_customer_data(customer_data_path):
    logger.info(f"Reading customer data from {customer_data_path}")
    customers = []
    buffer = []
    brace_count = 0

    with open(customer_data_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            buffer.append(line)
            brace_count += line.count('{') - line.count('}')
            if brace_count == 0 and buffer:
                try:
                    customer = json.loads(''.join(buffer))
                    if isinstance(customer, dict):
                        customers.append(customer)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON object: {e}")
                buffer = []

    if not customers:
        logger.error("No valid customer records found")
        raise ValueError("No valid customer records")

    invalid_records = []
    anonymized_data = []
    for customer in customers:
        email = customer.get('email')
        if not email or '@' not in email:
            invalid_records.append(customer)
            continue
        user_id = hashlib.sha256(email.encode()).hexdigest()
        sex = customer.get('sex', '').strip().lower()
        sex = 'M' if sex in ['male', 'm'] else 'F' if sex in ['female', 'f'] else None
        address = customer.get('address', {})
        postcode = standardize_postcode(address.get('postcode') if isinstance(address, dict) else None)
        if not postcode:
            invalid_records.append(customer)
            continue
        anonymized_data.append({
            'user_id': user_id,
            'sex': sex,
            'postcode': postcode
        })

    if invalid_records:
        with open('invalid_records.json', 'w') as f:
            json.dump(invalid_records, f)
        logger.info(f"Logged {len(invalid_records)} invalid records to invalid_records.json")

    if not anonymized_data:
        logger.error("No valid anonymized records")
        raise ValueError("No valid anonymized records")

    logger.info(f"Anonymized {len(anonymized_data)} customer records")
    return pd.DataFrame(anonymized_data)

# Process postcode data with correct column names
def process_postcode_data(postcode_data_path):
    logger.info("Reading postcode data from %s", postcode_data_path)
    try:
        df = pd.read_csv(postcode_data_path, usecols=['pcds', 'laua'], encoding='utf-8')
        df = df.rename(columns={'pcds': 'postcode', 'laua': 'local_authority'})
        df['postcode'] = df['postcode'].apply(standardize_postcode)
        df = df.dropna(subset=['postcode'])
        logger.info("Processed %d postcode records", len(df))
        return df
    except Exception as e:
        logger.error("Failed to process postcode data: %s", e)
        raise

# Set up database with indexes
def setup_database(db_path='analytics.db'):
    logger.info("Setting up database at %s", db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            user_id TEXT PRIMARY KEY,
            sex TEXT,
            postcode TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS postcodes (
            postcode TEXT PRIMARY KEY,
            local_authority TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_postcode ON customers (postcode)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_postcodes_postcode ON postcodes (postcode)")
    conn.commit()
    return conn

# Load data into database
def load_data_to_db(conn, customers_df, postcodes_df):
    logger.info("Loading data into database")
    customers_df.to_sql('customers', conn, if_exists='replace', index=False)
    postcodes_df.to_sql('postcodes', conn, if_exists='replace', index=False)
    conn.commit()

# Query users by local authority
def query_users_by_local_authority(conn):
    query = """
    SELECT p.local_authority, COUNT(DISTINCT c.user_id) AS user_count
    FROM customers c
    JOIN postcodes p ON c.postcode = p.postcode
    GROUP BY p.local_authority
    """
    result = pd.read_sql_query(query, conn)
    logger.info("Users by local authority:\n%s", result.to_string())
    return result

def query_users_by_sex(conn):
    query = """
    SELECT sex, COUNT(DISTINCT user_id) AS user_count
    FROM customers
    WHERE sex IS NOT NULL
    GROUP BY sex
    """
    result = pd.read_sql_query(query, conn)
    logger.info("Users by sex:\n%s", result.to_string())
    return result

# Main function with command-line args
def main(customer_data_url=None, postcode_data_url=None):
    # Allow args from CLI or function call (e.g., Airflow)
    customer_data_url = customer_data_url or sys.argv[1]
    postcode_data_url = postcode_data_url or sys.argv[2]
    if not (customer_data_url and postcode_data_url):
        print("Usage: python pipeline.py <customer_data_url> <postcode_data_url>")
        sys.exit(1)

    # Download data from URL (and extract zip if needed)
    customer_data_path = fetch_data_from_url(customer_data_url)
    postcode_data_path = fetch_data_from_url(postcode_data_url)

    customers_df = anonymize_customer_data(customer_data_path)
    postcodes_df = process_postcode_data(postcode_data_path)
    conn = setup_database()
    load_data_to_db(conn, customers_df, postcodes_df)
    query_users_by_local_authority(conn)
    query_users_by_sex(conn)
    conn.close()
    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()