# Import the required modules
import os
import re

import psycopg2
import xlrd
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Connect to the postgresql database
# Get the postgresql connection variables from environment variables
host = os.environ['PGHOST']
user = os.environ['PGUSER']
password = os.environ['PGPASSWORD']
port = os.environ['PGPORT']
database = os.environ['PGDATABASE']

# Connect to the postgresql database using the connection variables
conn = psycopg2.connect(host=host, user=user, password=password, port=port, database=database)
cur = conn.cursor()

# Get a list of all files in the directory
data_files = os.listdir('data_files/city_data')

# Loop through all files
for file_name in data_files:
    # Check if the file is an .xlsx file
    if file_name.endswith('.xlsx'):
        # Construct the full file path
        file_path = os.path.join('data_files/city_data', file_name)

        # Open the xlsx file
        book = xlrd.open_workbook(file_path)

        # Get the first sheet
        sheet = book.sheet_by_index(0)

        # Extract the city name from the file name
        city_name = os.path.basename(file_name).split(".")[0]

        # Insert the city name into the City table if it does not exist already
        cur.execute(
            "INSERT INTO City (cityName) VALUES (%s) ON CONFLICT (cityName) DO UPDATE SET cityName = EXCLUDED.cityName",
            (city_name,))
        conn.commit()

        # Get the cityid of the inserted city
        cur.execute("SELECT cityid FROM City WHERE cityName = %s", (city_name,))
        city_id = cur.fetchone()[0]

        # Loop through the rows of the sheet
        for row in range(1, sheet.nrows):
            # Get the category name from the category column
            category_name = sheet.cell_value(row, 0)

            # Insert the category name into the Category table if it does not exist already
            cur.execute(
                "INSERT INTO expensecategory (categoryname) VALUES (%s) ON CONFLICT (categoryname) DO UPDATE SET categoryname = EXCLUDED.categoryname",
                (category_name,))
            conn.commit()

            # Get the categoryid of the inserted category
            cur.execute("SELECT categoryid FROM expensecategory WHERE categoryName = %s", (category_name,))
            category_id = cur.fetchone()[0]

            # Get the title, priceAverage, priceMin, and priceMax from the sheet
            title = sheet.cell_value(row, 1)

            price_average = float(re.search(r'\d+\.?\d*', sheet.cell_value(row, 2)).group())
            price_min = float(str(sheet.cell_value(row, 3)).replace(',', '').replace(' ', ''))
            price_max = float(str(sheet.cell_value(row, 4)).replace(',', '').replace(' ', ''))

            # Insert the data into the Expense table
            cur.execute(
                "INSERT INTO Expense (cityid, categoryid, title, priceAverage, priceMin, priceMax) VALUES (%s, %s, %s, %s, %s, %s)",
                (city_id, category_id, title, price_average, price_min, price_max))
            conn.commit()

# Close the connection
conn.close()
