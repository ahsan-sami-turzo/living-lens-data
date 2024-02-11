from sqlalchemy import create_engine

from database import get_db_url, connect_to_db, close_connection
from models import Base

# Connect to the database
conn, cur = connect_to_db()

# Get the database URL
db_url = get_db_url()

# Create engine
engine = create_engine(db_url)

# Drop the tables if they exist
Base.metadata.drop_all(engine)

# Create all tables in the engine
Base.metadata.create_all(engine)

# Close the database connection
close_connection(conn)
