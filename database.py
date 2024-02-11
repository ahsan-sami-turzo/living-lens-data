import os

import psycopg2
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Get the postgresql connection variables from environment variables
host = os.environ['PGHOST']
user = os.environ['PGUSER']
password = os.environ['PGPASSWORD']
port = os.environ['PGPORT']
database = os.environ['PGDATABASE']


def connect_to_db():
    # Connect to the postgresql database using the connection variables
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, database=database)
    cur = conn.cursor()
    return conn, cur


def close_connection(conn):
    # Close the connection
    conn.close()
