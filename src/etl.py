import os
import time 
import pandas as pd
from datetime import datetime

from .config import RAW_DATA_PATH, PROCESSED_DATA_PATH, SANCTIONED_COUNTRIES
from .validation import validate_schema, validate_nulls, validate_business_rules
import logging
from datetime import datetime, UTC


def setup_logging():
    log_folder = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "logs"
    )
    os.makedirs(log_folder, exist_ok=True)

    log_file = os.path.join(log_folder, "pipeline.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
def log_pipeline_metadata(file_name, records_processed, processing_time, status):

    metadata_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "logs",
        "pipeline_metadata.csv"
    )

    file_exists = os.path.exists(metadata_file)

    with open(metadata_file, "a") as f:

        if not file_exists:
            f.write("file_name,records_processed,processing_time,status\n")

        f.write(
            f"{file_name},{records_processed},{processing_time},{status}\n"
        )
def extract():
   
    """
    Detect all raw CSV files in raw folder
    """
    raw_files = [
        f for f in os.listdir(RAW_DATA_PATH)
        if f.endswith(".csv")
    ]

    logging.info(f"Found {len(raw_files)} raw files")

    return raw_files
   
def get_processed_files():
    tracking_file = os.path.join(PROCESSED_DATA_PATH, "processed_files.txt")

    if not os.path.exists(tracking_file):
        return set()

    with open(tracking_file, "r") as f:
        return set(line.strip() for line in f.readlines())

def mark_file_processed(filename):
    tracking_file = os.path.join(PROCESSED_DATA_PATH, "processed_files.txt")

    with open(tracking_file, "a") as f:
        f.write(filename + "\n")

def transform(df: pd.DataFrame):
    """
    Apply compliance and cleaning transformations.
    """

    # Standardize column names
    df.columns = df.columns.str.lower()

    # Validate schema
    validate_schema(df)
    validate_nulls(df)
    validate_business_rules(df)

    # Remove sanctioned countries
    df = df[~df["country"].isin(SANCTIONED_COUNTRIES)]

    # Convert amount to float
    df["amount"] = df["amount"].astype(float)

    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.now(UTC)

    return df


def load(df: pd.DataFrame):
    """
    Write partitioned parquet files by trade_date.
    """

    for trade_date, partition_df in df.groupby("trade_date"):
        partition_folder = os.path.join(
            PROCESSED_DATA_PATH,
            f"trade_date={trade_date}"
        )

        os.makedirs(partition_folder, exist_ok=True)

        output_path = os.path.join(partition_folder, "trades.parquet")

        partition_df.to_parquet(output_path, index=False)

        logging.info(f"Written partition: {output_path}")


def run_pipeline():
    setup_logging()

    try:
        raw_files = extract()
        processed_files = get_processed_files()

        for file in raw_files:
            start_time = time.time()

            if file in processed_files:
                logging.info(f"Skipping already processed file: {file}")
                continue

            file_path = os.path.join(RAW_DATA_PATH, file)

            df = pd.read_csv(file_path)
            record_count = len(df)
            df_transformed = transform(df)

            load(df_transformed)
            processing_time = round(time.time() - start_time, 2)

            log_pipeline_metadata(
                 file,
                record_count,
                processing_time,
                "SUCCESS"
)

            mark_file_processed(file)

            logging.info(f"Finished processing file: {file}")

    except Exception as e:
        processing_time = round(time.time() - start_time, 2)

        log_pipeline_metadata(
        file,
        0,
        processing_time,
        "FAILED"
)
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()