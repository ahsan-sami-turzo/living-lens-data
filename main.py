import data_loader
from sqlalchemy import create_engine
from database import get_db_url, connect_to_db, close_connection
from models import Base

import json
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Define the column names for the excel files
columns = ["Country", "City", "Category", "Title", "priceAverage", "priceMin", "priceMax"]

# Define a list of unwanted characters
unwanted_chars = ["\n", "\t", "\r", "\u00a0", "?", "€", "\xa0", "$", "kr", "Ft", "zł"]


def remove_unwanted_chars(string):
    # Replace each unwanted character with an empty string
    for char in unwanted_chars:
        string = string.replace(char, "")
    # Return the cleaned string
    return string


def remove_unwanted_chars_from_number(number):
    number = str(number)
    number = remove_unwanted_chars(number)
    number = number.replace("\u20ac", "").replace("$", "").replace(",", "")
    if number == "":
        return 0
    number = float(number)
    return number


class ExtractTable:

    def __init__(self, page):
        self.table = page.find("table", class_="data_wide_table")
        self.data = {}

    def extract(self):
        if not self.table:
            return None

        for row in self.table.find_all("tr"):
            if row.find_all("th"):
                header = row.find_all("th")[0].text
                self.data[header] = []
            else:
                self.data[header].append([cell.text for cell in row.find_all("td")])

        return self.data


class CityScraper:

    def __init__(self, base_url, country, scrape_all_cities=False):
        self.base_url = base_url
        self.country = country
        self.all_cities = []
        self.results = {}

        # Scrape country-level data
        self.url = f"{base_url}/country_result.jsp?displayCurrency=EUR&country={country}"
        response = requests.get(self.url)
        if response.status_code == 200:
            self.soup = BeautifulSoup(response.content, "html.parser")
            self.extract_country_data()
            if scrape_all_cities:
                self.extract_all_cities()

    def write_city_data_to_json(self, city_data, filename):
        try:
            with open(filename, "w") as f:
                json.dump(city_data, f, indent=4)  # Add indentation for readability
        except Exception as e:
            print(f"Error writing data to JSON: {e}")

    def write_city_data_to_excel(self, city_data, filename):
        try:
            first_row_length = len(list(city_data.values())[0])
            for row in city_data.values():
                if len(row) != first_row_length:
                    raise ValueError("Inconsistent data lengths in city_data")

            df = pd.DataFrame(city_data)
            writer = pd.ExcelWriter(filename, engine='xlsxwriter')
            df.to_excel(writer, sheet_name=filename, index=False)
            writer.save()
        except ValueError as e:
            print(f"Error writing data: {e}")

    def extract_country_data(self):
        extractor = ExtractTable(self.soup)
        self.results[self.country] = extractor.extract()

    def extract_city_data(self, city):
        city_url = f"{self.base_url}/city_result.jsp?country={self.country}&city={city}"
        response = requests.get(city_url)
        if response.status_code == 200:
            city_soup = BeautifulSoup(response.content, "html.parser")
            extractor = ExtractTable(city_soup)
            return extractor.extract()
        else:
            print(f"Error fetching data for city: {city}")
            return None

    def extract_all_cities(self):
        self.all_cities_form = self.soup.find("form", class_="standard_margin")
        if self.all_cities_form:
            self.all_cities = [option["value"] for option in self.all_cities_form.find_all("option")]
            self.results[self.country]["cities"] = {}
            for city in self.all_cities:
                data = self.extract_city_data(city)
                if data:
                    self.results[self.country]["cities"][city] = data
                    # Writes data to excel
                    # self.write_city_data_to_excel(data, f"{city}.xlsx")

                    # Create Country Directory
                    if not os.path.exists(f"data_files/numbeo_data/{self.country}"):
                        os.makedirs(f"data_files/numbeo_data/{self.country}")

                    # Writes data to json
                    json_file = f"data_files/numbeo_data/{self.country}/{city}.json"
                    self.write_city_data_to_json(data, json_file)

                    # Construct the full path of the excel file with the same name
                    excel_file = os.path.join(f"data_files/numbeo_data/{self.country}/{city}.xlsx")
                    if os.path.exists(excel_file):
                        os.remove(excel_file)

                    # Read the json file
                    with open(json_file, "r") as file:
                        data = json.load(file)

                    # Create an empty list to store the rows
                    rows = []

                    # Iterate over the json data
                    for category, category_data in data.items():
                        # Iterate over the list of prices
                        for price_data in category_data:
                            # Extract the title, average price, and price range
                            title = price_data[0].strip()
                            average_price = price_data[1].strip()
                            price_range = price_data[2].strip()

                            # Split the price range by the dash
                            price_range = price_range.split("-")

                            # If the price range has two values, assign them to min and max
                            if len(price_range) == 2:
                                min_price = price_range[0].strip()
                                max_price = price_range[1].strip()
                            # Otherwise, assign the average price to both min and max
                            else:
                                min_price = average_price
                                max_price = average_price

                            # Remove unwanted characters from the strings and the numbers
                            category = remove_unwanted_chars(category)
                            title = remove_unwanted_chars(title)
                            average_price = remove_unwanted_chars_from_number(average_price)
                            min_price = remove_unwanted_chars_from_number(min_price)
                            max_price = remove_unwanted_chars_from_number(max_price)

                            # Create a row as a dictionary
                            row = {
                                "Country": self.country,
                                "City": city,
                                "Category": category,
                                "Title": title,
                                "priceAverage": average_price,
                                "priceMin": min_price,
                                "priceMax": max_price
                            }

                            # Append the row to the list
                            rows.append(row)

                    # Convert the list of rows to a pandas dataframe
                    df = pd.DataFrame(rows, columns=columns)

                    # Write the dataframe to the excel file
                    df.to_excel(excel_file, index=False)

                    # Print a success message
                    print(f"The data from {json_file} was successfully stored to {excel_file}.")

    def get_results(self):
        return self.results


if __name__ == "__main__":
    URL = "https://www.numbeo.com/cost-of-living/"

    # Create Directory
    if not os.path.exists(f"data_files"):
        os.makedirs(f"data_files")
    if not os.path.exists(f"data_files/numbeo_data"):
        os.makedirs(f"data_files/numbeo_data")

    COUNTRY = "Finland"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Sweden"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Norway"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Germany"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Hungary"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Italy"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Portugal"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Greece"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "France"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Iceland"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Denmark"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")

    COUNTRY = "Netherlands"
    SCRAPE_ALL_CITIES = True  # Set to True to scrape all cities
    scraper = CityScraper(URL, COUNTRY, scrape_all_cities=SCRAPE_ALL_CITIES)
    results = scraper.get_results()
    with open("results.json", "w") as f:
        json.dump(results, f)
    print(f"Scraped data for {COUNTRY}:")


    # Connect to the database
    conn, cur = connect_to_db()

    # create_tables
    db_url = get_db_url()
    engine = create_engine(db_url)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # Load the data
    COUNTRY = "Finland"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Sweden"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Norway"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Germany"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Hungary"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Italy"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Portugal"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Greece"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "France"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Iceland"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Denmark"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    COUNTRY = "Netherlands"
    data_loader.load_cities(COUNTRY)
    data_loader.update_subcategory_table(COUNTRY)
    data_loader.load_city_data(COUNTRY)

    # Close the database connection
    close_connection(conn)