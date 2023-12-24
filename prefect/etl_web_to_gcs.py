from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
import ssl
import requests
import gzip
from io import BytesIO

# Set the path to the CA certificate bundle
os.environ['REQUESTS_CA_BUNDLE'] = '/Users/himanshutalele/anaconda3/lib/python3.11/site-packages/certifi/cacert.pem'



@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into a pandas DataFrame"""
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        # df = pd.read_csv(dataset_url)
        # Download the file
        response = requests.get(dataset_url)
        # Check if the request was successful
        response.raise_for_status()  

        # Check if the file is gzipped
        if dataset_url.endswith(".gz"):
            # Decompress the gzipped content
            compressed_data = BytesIO(response.content)
            with gzip.GzipFile(fileobj=compressed_data, mode='rb') as f:
                content = f.read()
        else:
            # If not gzipped, use the content directly
            content = response.content

        # Convert the content to a DataFrame
        df = pd.read_csv(BytesIO(content))

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise
    return df

# @task(log_prints=True)
# def clean(df = pd.DataFrame) -> pd.DataFrame:
#     """Fix Data Type issues"""
#     df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
#     df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
#     print(df.head(2))
#     print(f"Columns: {df.dtypes}")
#     print(f"Rows: {len(df)}")
#     return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as a parquet file"""
    # Create the directory if it doesn't exist
    directory_path = Path(f"data/{color}/")
    directory_path.mkdir(parents=True, exist_ok=True)
    # Create the file path
    path = directory_path / f"{dataset_file}.parquet"
    # # Create a folder data/green in the working directory before running this code
    # path = Path(f"data/{color}/{dataset_file}.parquet")   
    df.to_parquet(path, compression="gzip")
    # Checking to see if the slashes are forward. Default is backwards in windows
    print(path.as_posix())
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("dte-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path.as_posix()) 
    #gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

    return

@flow()
def etl_web_to_gcs() -> None:
    """The Main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/himanshuTaleleNeu/data/raw/main/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()