import os
import tempfile
import requests
import zipfile
import shutil
import logging

logger = logging.getLogger(__name__)

def fetch_data_from_url(url):
    """Fetch data from a URL, extracting ZIP files if needed."""
    logger.info(f"Fetching data from {url}")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    tmp_dir = tempfile.mkdtemp()
    filename = os.path.basename(url)
    file_path = os.path.join(tmp_dir, filename)

    # Save the file in chunks to avoid memory blowup
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # If it's a zip file, extract and return the inner file path
    if filename.endswith('.zip'):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            extracted_dir = os.path.join(tmp_dir, 'extracted')
            zip_ref.extractall(extracted_dir)
            os.remove(file_path)

            # Handle MacOS __MACOSX garbage
            extracted_files = []
            for root, _, files in os.walk(extracted_dir):
                for name in files:
                    if not name.startswith('.') and '__MACOSX' not in root:
                        extracted_files.append(os.path.join(root, name))

            if not extracted_files:
                raise Exception("No valid files found in ZIP")

            logger.info(f"Extracted files: {extracted_files}")
            return extracted_files[0]  # Just return the first valid file

    return file_path
