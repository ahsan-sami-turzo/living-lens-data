import os

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Load the environment variables from the .env file
load_dotenv()

# Get the postgresql connection variables from environment variables
host = os.environ['PGHOST']
user = os.environ['PGUSER']
password = os.environ['PGPASSWORD']
port = os.environ['PGPORT']
database = os.environ['PGDATABASE']


def get_db_url():
    # Construct the database URL
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return db_url


def create_session():
    # Create engine
    engine = create_engine(get_db_url())

    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def connect_to_db():
    # Connect to the postgresql database using the connection variables
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, database=database)
    cur = conn.cursor()
    return conn, cur


def close_connection(conn):
    # Close the connection
    conn.close()
