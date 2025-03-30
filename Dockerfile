FROM apache/airflow:2.6.3

# Switch to the airflow user
USER airflow

RUN pip install yfinance python-dotenv supabase boto3
