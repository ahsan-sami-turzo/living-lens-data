import os
import re
import sys
import traceback

import pandas as pd
import xlrd

from database import create_session
from models import Lifestyle, Country, City, Category, SubCategory, Price


def load_cities():
    # Get a list of all files in the directory
    data_files = os.listdir('data_files/city_data')

    # Create session
    session = create_session()

    try:
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

                # Check if the city already exists
                existing_city = session.query(City).filter_by(city_name=city_name).first()

                if existing_city is None:
                    # If the city does not exist, create a new city
                    new_city = City(name=city_name)

                    # Add the new city to the session
                    session.add(new_city)

                    # Commit the changes
                    session.commit()
                    print(f"City '{city_name}' has been added successfully.")
                else:
                    print(f"City '{city_name}' already exists.")

    except Exception as e:
        # Rollback the changes on error
        session.rollback()
        print(f"An error occurred: {e}")

    finally:
        # Close the session
        session.close()


def load_lifestyle_data():
    # Create session
    session = create_session()

    try:
        path = "data_files/lifestyle_data/Lifestyle.xlsx"
        base_df = pd.read_excel(path, na_values="nan", usecols="A:E")

        # Load countries' data
        countries = base_df.iloc[:, 0]
        for country in countries:
            if str(country) != 'nan':
                temp = Country(str(country))
                session.add(temp)

        # Load cities' data
        cities = base_df.iloc[:, 1]
        for city in cities:
            if str(city) != 'nan':
                temp = City(str(city))
                session.add(temp)

        # Load lifestyles' data
        lifestyles = base_df.iloc[:, 2]
        for lifestyle in lifestyles:
            if str(lifestyle) != 'nan':
                temp = Lifestyle(str(lifestyle))
                session.add(temp)

        # Load categories' data
        categories = base_df.iloc[:, 3]
        for category in categories:
            if str(category) != 'nan':
                temp = Category(str(category))
                session.add(temp)

        # Load subcategories' data
        subcategories = base_df.iloc[:, 4]
        for subcategory in subcategories:
            if str(subcategory) != 'nan':
                temp = SubCategory(str(subcategory))
                session.add(temp)

        # Update city table with foreign keys based on country data
        session.query(City).filter(City.city_name.in_(['Berlin', 'Frankfurt', 'Munich', 'Münich'])).update(
            {City.country_id_fk: 1}, synchronize_session=False)
        session.query(City).filter(City.city_name.in_(['Milan', 'Rome', 'Venice'])).update({City.country_id_fk: 2},
                                                                                           synchronize_session=False)
        session.query(City).filter(City.city_name.in_(['Helsinki', 'Lappeenranta', 'Lahti'])).update({City.country_id_fk: 3},
                                                                                            synchronize_session=False)

        session.commit()

    except Exception as e:
        # Rollback the changes on error
        session.rollback()
        # Get current exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Extract stack entries
        traceback_details = traceback.extract_tb(exc_traceback)
        # Get filename and line number of the most recent stack entry
        filename, line_number, func_name, text = traceback_details[-1]
        print(f"An error occurred in file {filename} on line {line_number}: {e}")

    finally:
        # Close the session
        session.close()


def update_subcategory_table():
    # Get a list of all files in the directory
    data_files = os.listdir('data_files')

    # Create session
    session = create_session()

    try:
        # Loop through all files
        for file_name in data_files:
            # Check if the file is an .xlsx file
            if file_name.endswith('.xlsx'):
                # Construct the full file path
                file_path = os.path.join('data_files', file_name)

                # Open the xlsx file
                df = pd.read_excel(file_path)

                # Loop through the rows of the DataFrame
                counter = 1
                for subcategory in df['SubCategory']:
                    if str(subcategory) == "Restaurants":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 1},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Markets":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 2},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Transportation":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 3},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Utilities (Monthly)":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 4},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Sports And Leisure":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 5},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Childcare":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 6},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Clothing And Shoes":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 7},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Rent Per Month":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 8},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Buy Apartment Price":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 9},
                            synchronize_session=False)
                        counter = counter + 1
                    elif str(subcategory) == "Salaries And Financing":
                        session.query(SubCategory).filter(SubCategory.id == counter).update(
                            {SubCategory.category_id_fk: 10}, synchronize_session=False)
                        counter = counter + 1

        session.commit()

    except Exception as e:
        # Rollback the changes on error
        session.rollback()
        # Get current exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Extract stack entries
        traceback_details = traceback.extract_tb(exc_traceback)
        # Get filename and line number of the most recent stack entry
        filename, line_number, func_name, text = traceback_details[-1]
        print(f"An error occurred in file {filename} on line {line_number}: {e}")

    finally:
        # Close the session
        session.close()


def load_city_data():
    # Get a list of all files in the directory
    data_files = os.listdir('data_files/city_data')

    # Create session
    session = create_session()

    try:
        # Loop through all files
        for file_name in data_files:
            # Check if the file is an .xlsx file
            if file_name.endswith('.xlsx'):
                # Construct the full file path
                file_path = os.path.join('data_files/city_data', file_name)

                # Open the xlsx file
                df = pd.read_excel(file_path)

                # Extract the city name from the file name
                city_name = os.path.splitext(file_name)[0]

                # Get the city_id of the city
                # city_id = session.query(City.id).filter_by(city_name=city_name).first()[0]
                city_query = session.query(City.id).filter_by(city_name=city_name).first()
                if city_query is not None:
                    city_id = city_query[0]
                else:
                    print(f"No city found with name {city_name}")
                    continue

                # Loop through the rows of the DataFrame
                for index, row in df.iterrows():
                    # Get the category name from the category column
                    category_name = row.iloc[0]

                    # Get the category_id of the category
                    # category_id = session.query(Category.id).filter_by(category_name=category_name).first()[0]
                    category_query = session.query(Category.id).filter_by(category_name=category_name).first()
                    if category_query is not None:
                        category_id = category_query[0]
                    else:
                        print(f"No category found with name {category_name}")
                        continue

                    # Get the title from the row
                    title = row.iloc[1]

                    # Get the average_price, price_min, and price_max from the row
                    match_average = re.search(r'\d+\.?\d*', str(row.iloc[2]))
                    price_average = float(match_average.group()) if match_average else None

                    match_min = re.search(r'\d+\.?\d*', str(row.iloc[3]))
                    price_min = float(match_min.group()) if match_min else None

                    match_max = re.search(r'\d+\.?\d*', str(row.iloc[4]))
                    price_max = float(match_max.group()) if match_max else None

                    # Insert the data into the Price table
                    price = Price(city_id, category_id, price_average, price_min, price_max)
                    session.add(price)

        session.commit()

    except Exception as e:
        # Rollback the changes on error
        session.rollback()
        # Get current exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Extract stack entries
        traceback_details = traceback.extract_tb(exc_traceback)
        # Get filename and line number of the most recent stack entry
        filename, line_number, func_name, text = traceback_details[-1]
        print(f"An error occurred in file {filename} on line {line_number}: {e}")

    finally:
        # Close the session
        session.close()
