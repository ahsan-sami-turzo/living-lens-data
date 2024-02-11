import os
import re

import xlrd


def process_excel_files(cur, conn):
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
            try:

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

                    # Use regular expressions to extract the first number in the cell value
                    # If no number is found, default to 0.0
                    price_average = sheet.cell_value(row, 2)
                    if not isinstance(price_average, float):
                        price_average_match = re.search(r'\d+\.?\d*', price_average)
                        price_average = float(price_average_match.group()) if price_average_match else 0.0

                    price_min = sheet.cell_value(row, 3)
                    if not isinstance(price_min, float):
                        price_min_match = re.search(r'\d+\.?\d*', price_min)
                        price_min = float(price_min_match.group()) if price_min_match else 0.0

                    price_max = sheet.cell_value(row, 4)
                    if not isinstance(price_max, float):
                        price_max_match = re.search(r'\d+\.?\d*', price_max)
                        price_max = float(price_max_match.group()) if price_max_match else 0.0

                    # Insert the data into the Expense table
                    cur.execute(
                        "INSERT INTO Expense (cityid, categoryid, title, priceAverage, priceMin, priceMax) VALUES (%s, %s, %s, %s, %s, %s)",
                        (city_id, category_id, title, price_average, price_min, price_max))
                    conn.commit()

                print(f"success")
            except Exception as e:
                # If an error occurs, rollback the transaction
                conn.rollback()
                print(f"An error occurred: {e}")
