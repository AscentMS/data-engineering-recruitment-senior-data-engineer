import json
import pandas as pd
import sqlite3
import hashlib

def anonymize_data(value, salt="xyz_salt"):
    """Anonymize sensitive data with SHA-256 hashing."""
    if not value or pd.isna(value):
        return None
    
    value_to_hash = str(value) + salt
    return hashlib.sha256(value_to_hash.encode()).hexdigest()

def parse_customer_data(file_path):
    """Parse the customer data file (concatenated JSON objects)."""
    print(f"Reading customer data from {file_path}...")
    
    customers = []
    current_json = ""
    open_braces = 0
    
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            open_braces += line.count('{') - line.count('}')
            current_json += line
            
            if open_braces == 0 and current_json:
                try:
                    customer = json.loads(current_json)
                    customers.append(customer)
                except json.JSONDecodeError:
                    pass  # Skip invalid JSON objects
                current_json = ""
    
    print(f"Found {len(customers)} customer records")
    return customers

def process_customers(customers):
    """Process and anonymize customer data."""
    processed_customers = []
    
    for customer in customers:
        # Extract and clean postcode
        postcode = None
        if 'address' in customer and 'postcode' in customer['address']:
            postcode = customer['address']['postcode'].strip()
        
        # Create anonymized customer record
        processed_customer = {
            'customer_id': anonymize_data(customer.get('email')),
            'sex': customer.get('sex'),
            'postcode': postcode
        }
        processed_customers.append(processed_customer)
    
    return processed_customers

def load_postcode_data(file_path):
    """Load postcode data from CSV file."""
    print(f"Reading postcode data from {file_path}...")
    
    try:
        # Read the CSV file
        postcodes_df = pd.read_csv(file_path)
        
        # Select only the columns we need (postcode and local authority)
        if 'pcd' in postcodes_df.columns and 'laua' in postcodes_df.columns:
            postcodes_df = postcodes_df[['pcd', 'laua']]
            postcodes_df.columns = ['postcode', 'local_authority']
        else:
            print("Warning: Expected columns 'pcd' and 'laua' not found. Creating empty DataFrame.")
            # Create an empty DataFrame with the right columns if required columns not found
            postcodes_df = pd.DataFrame(columns=['postcode', 'local_authority'])
        
        # Clean postcode data (only if not empty)
        if not postcodes_df.empty:
            postcodes_df['postcode'] = postcodes_df['postcode'].astype(str).str.strip()
        
        print(f"Processed {len(postcodes_df)} postcode records")
        return postcodes_df
        
    except Exception as e:
        print(f"Error loading postcode data: {e}")
        return pd.DataFrame(columns=['postcode', 'local_authority'])

def create_database(customers, postcodes_df):
    """Create SQLite database and load the data."""
    print("Creating database...")
    
    # Create or connect to SQLite database
    conn = sqlite3.connect('analytics.db')
    
    # Create customers table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        sex TEXT,
        postcode TEXT
    )
    ''')
    
    # Create postcodes table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS postcodes (
        postcode TEXT PRIMARY KEY,
        local_authority TEXT
    )
    ''')
    
    # Insert postcode data
    postcodes_df.to_sql('postcodes', conn, if_exists='replace', index=False)
    
    # Insert customer data
    cursor = conn.cursor()
    for customer in customers:
        try:
            cursor.execute(
                'INSERT OR REPLACE INTO customers (customer_id, sex, postcode) VALUES (?, ?, ?)',
                (customer['customer_id'], customer['sex'], customer['postcode'])
            )
        except Exception as e:
            print(f"Error inserting customer: {e}")
    
    # Create a view for easier querying
    conn.execute('''
    CREATE VIEW IF NOT EXISTS users_by_authority AS
    SELECT 
        p.local_authority,
        c.customer_id
    FROM 
        customers c
    LEFT JOIN 
        postcodes p ON c.postcode = p.postcode
    ''')
    
    conn.commit()
    conn.close()
    print("Database created successfully")

def count_users_by_authority():
    """Count distinct users by local authority."""
    conn = sqlite3.connect('analytics.db')
    
    # Load SQL query from separate file
    with open('query.sql', 'r') as f:
        query = f.read()
    
    results_df = pd.read_sql_query(query, conn)
    conn.close()
    
    return results_df

def main():
    """Main function to run the data pipeline."""
    # File paths
    customer_file = 'data/users.json'
    postcode_file = 'data/postcodes.csv'
    
    # Process data
    customers = parse_customer_data(customer_file)
    processed_customers = process_customers(customers)
    postcodes_df = load_postcode_data(postcode_file)
    
    # Create database
    create_database(processed_customers, postcodes_df)
    
    # Get results
    results = count_users_by_authority()
    print("\nUsers by Local Authority:")
    print(results)
    
    print("\nData pipeline completed successfully")

if __name__ == "__main__":
    main()