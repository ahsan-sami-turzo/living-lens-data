import create_tables
import data_loader
from database import connect_to_db, close_connection
from excel_processing import process_excel_files

# Connect to the database
conn, cur = connect_to_db()

# Run the create_tables.py module to create and truncate the tables
create_tables

# Load the data
data_loader.load_lifestyle_data()

# Process the Excel files
# process_excel_files(cur, conn)

# Close the database connection
close_connection(conn)
