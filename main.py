from database import connect_to_db, close_connection
from excel_processing import process_excel_files

# Connect to the database
conn, cur = connect_to_db()

# Process the Excel files
process_excel_files(cur, conn)

# Close the database connection
close_connection(conn)