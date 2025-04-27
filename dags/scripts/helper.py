import os
import pandas as pd
import kagglehub
from io import  BytesIO
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

gcs_client = storage.Client()
bucket = gcs_client.bucket(GCS_BUCKET_NAME)

def upload_to_gcs(df, gcs_path):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    blob = bucket.blob(gcs_path)
    # Use resumable upload for large files
    blob.upload_from_file(csv_buffer, content_type='text/csv', rewind=True, timeout=600)
    print(f"File uploaded to gs://{GCS_BUCKET_NAME}/{gcs_path}")

def fetch_dataset(dataset_identifier, dataset_location):
    """
    Downloads a dataset from KaggleHub and loads a specific CSV file into a pandas DataFrame.

    Parameters:
    - dataset_identifier (str): The Kaggle dataset identifier (e.g., 'wordsforthewise/lending-club').
    - dataset_location (str): The relative path to the CSV file within the dataset.

    Returns:
    - full_csv_path (str): The full path to the CSV file within the dataset.
    """
    try:
        dataset_path = kagglehub.dataset_download(dataset_identifier)

        full_csv_path = os.path.join(dataset_path, dataset_location)

        print(f"Loaded dataset from {dataset_identifier}")
        print(pd.read_csv(full_csv_path).head())

        return full_csv_path

    except FileNotFoundError:
        print(f"File not found: {dataset_location} in {dataset_identifier}")
    except pd.errors.ParserError:
        print(f"Failed to parse CSV file: {dataset_location}")
    except Exception as e:
        print(f"An error occurred while fetching dataset: {e}")

    return None

def parse_dataframe(dataset_identifier, dataset_location, selected_columns):

    dataset_path = fetch_dataset(dataset_identifier, dataset_location)
    df = pd.read_csv(dataset_path)
    subset_df = df[selected_columns].dropna()

    return subset_df




