import pytest
import pandas as pd
from src.pipeline import standardize_postcode, anonymize_customer_data

def test_standardize_postcode():
    assert standardize_postcode("AB1 2CD") == "AB12CD"
    assert standardize_postcode("ab1 2cd") == "AB12CD"
    assert standardize_postcode("A") == None
    assert standardize_postcode("") == None
    assert standardize_postcode("Invalid123") == None
    assert standardize_postcode("SW1A 1AA") == "SW1A1AA"

def test_anonymize_customer_data(tmp_path):
    # Create a sample JSON file
    data = '''
    {"email": "valid@example.com", "sex": "Male", "address": {"postcode": "AB12 3CD"}}
    {"email": "invalid_email", "sex": "Female", "address": {"postcode": "XY34 5ZT"}}
    {"email": "missing@domain.com", "sex": "X", "address": {}}
    '''
    file_path = tmp_path / "test_customer_data.json"
    file_path.write_text(data)
    
    # Run the function
    df = anonymize_customer_data(str(file_path))
    
    # Check the results
    assert len(df) == 1  # Only the first record should be valid
    assert df['sex'].iloc[0] == 'M'
    assert df['postcode'].iloc[0] == 'AB123CD'
    assert 'email' not in df.columns  # Email should be anonymized to user_id