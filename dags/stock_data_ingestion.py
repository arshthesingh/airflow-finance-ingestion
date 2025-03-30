from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import time
import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
import logging
import sys
import boto3
from io import StringIO

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()
logging.debug("Loaded .env file")

# S3 config from env variables
S3_BUCKET = os.getenv("S3_BUCKET", "my-airflow-data-bucket")

def load_tickers_from_csv(file_path):
    """
    Load tickers from a CSV file.
    The CSV file should have a header named 'Ticker'.
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        tickers_csv = os.path.join(current_dir, file_path)
        logging.debug(f"Loading tickers from: {tickers_csv}")
        df = pd.read_csv(tickers_csv)
        tickers = df["Ticker"].dropna().tolist()
        logging.debug(f"Loaded tickers: {tickers}")
        return tickers
    except Exception as e:
        logging.error("Error loading tickers.csv", exc_info=True)
        raise

def fetch_data(tickers, batch_size=100):
    """
    Fetch stock data from yfinance in batches for the past month and reshape it to long format.
    """
    try:
        # Fetch data for the past 30 days
        start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        logging.debug(f"Start date for monthly data fetching: {start_date}")

        batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
        logging.debug(f"Total batches to process: {len(batches)}")
        all_data = []

        for idx, batch in enumerate(batches):
            logging.debug(f"Fetching batch {idx+1}/{len(batches)} with tickers: {batch}")
            try:
                batch_data = yf.download(batch, start=start_date, group_by="ticker", progress=False)
                missing_tickers = [ticker for ticker in batch if ticker not in batch_data.columns.get_level_values(0)]
                if missing_tickers:
                    logging.debug(f"Missing data for tickers in batch {idx+1}: {missing_tickers}")
                all_data.append(batch_data)
                logging.debug(f"Batch {idx+1} completed successfully.")
            except Exception as e:
                logging.error(f"Error fetching batch {idx+1}: {e}", exc_info=True)
            time.sleep(1)

        if all_data:
            combined_data = pd.concat(all_data, axis=1)
            logging.debug(f"Combined data shape: {combined_data.shape}")

            if isinstance(combined_data.columns, pd.MultiIndex):
                stacked = combined_data.stack(level=0)
                stacked.index.names = ['Date', 'Stock']
                long_format = stacked.reset_index()
            else:
                long_format = combined_data.reset_index()
                long_format['Stock'] = tickers[0]

            if 'Adj Close' not in long_format.columns and 'Close' in long_format.columns:
                logging.debug("Using 'Close' as 'Adj Close'")
                long_format['Adj Close'] = long_format['Close']
            elif 'Adj Close' in long_format.columns:
                if long_format['Adj Close'].isnull().all() and 'Close' in long_format.columns:
                    logging.debug("All 'Adj Close' values are null, replacing with 'Close'")
                    long_format['Adj Close'] = long_format['Close']
            if 'Close' in long_format.columns:
                long_format = long_format.drop(columns=['Close'])

            expected_columns = ['Date', 'Stock', 'Open', 'High', 'Low', 'Adj Close', 'Volume']
            long_format = long_format[[col for col in expected_columns if col in long_format.columns]]
            logging.debug(f"Final data columns: {long_format.columns.tolist()}")

            if 'Date' in long_format.columns:
                long_format['Date'] = long_format['Date'].astype(str)
            return long_format
        else:
            logging.debug("No data fetched from any batch.")
            return None
    except Exception as e:
        logging.error("Error in fetch_data", exc_info=True)
        raise

def upload_to_s3(data: pd.DataFrame, bucket_name: str, object_key: str):
    """
    Upload the DataFrame as a CSV file to the specified S3 bucket.
    """
    try:
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
        logging.debug(f"Data successfully uploaded to S3 as {object_key}!")
    except Exception as e:
        logging.error("Failed to upload data to S3", exc_info=True)
        raise

def fetch_and_upload_stock_data(**context):
    """
    Load tickers, fetch stock data, and upload the CSV file to S3.
    A dynamic filename is created using the DAG's execution date.
    """
    try:
        logging.debug("Starting fetch_and_upload_stock_data")
        tickers = load_tickers_from_csv("tickers.csv")
        logging.debug(f"Tickers loaded: {tickers}")
        data = fetch_data(tickers)
        if data is not None:
            logging.debug("Data fetched successfully. Preview:")
            logging.debug(f"\n{data.head()}")
            # Use the DAG's execution date to create a unique filename so data isn't overwritten
            execution_date = context['execution_date'].strftime('%Y%m%d')
            object_key = f"stocks_data_{execution_date}.csv"
            upload_to_s3(data, S3_BUCKET, object_key)
        else:
            logging.debug("No data fetched.")
    except Exception as e:
        logging.error("Error in fetch_and_upload_stock_data", exc_info=True)
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'stock_data_ingestion',
    default_args=default_args,
    description='Fetch monthly stock data and upload as CSV to S3 with dynamic filenames',
    schedule_interval='@monthly',
    catchup=False
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_stock_data',
    python_callable=fetch_and_upload_stock_data,
    provide_context=True,
    dag=dag
)

if __name__ == '__main__':
    fetch_and_upload_stock_data()
