import pandas as pd

from database import create_session
from models import Lifestyle, Country, City, Category, SubCategory


def load_data():
    path = "C:\\Data\\"
    base_df = pd.read_excel(path + "Lifestyle.xlsx", na_values="nan", usecols="A:E")

    # Create session
    session = create_session()

    # Load countries' data
    countries = base_df.iloc[:, 0]
    for country in countries:
        if str(country) != 'nan':
            temp = Country(country_name=str(country))
            session.add(temp)

    # Load cities' data
    cities = base_df.iloc[:, 1]
    for city in cities:
        if str(city) != 'nan':
            temp = City(city_name=str(city))
            session.add(temp)

    # Load lifestyles' data
    lifestyles = base_df.iloc[:, 2]
    for lifestyle in lifestyles:
        if str(lifestyle) != 'nan':
            temp = Lifestyle(lifestyle_name=str(lifestyle))
            session.add(temp)

    # Load categories' data
    categories = base_df.iloc[:, 3]
    for category in categories:
        if str(category) != 'nan':
            temp = Category(category_name=str(category))
            session.add(temp)

    # Load subcategories' data
    subcategories = base_df.iloc[:, 4]
    for subcategory in subcategories:
        if str(subcategory) != 'nan':
            temp = SubCategory(subcategory_name=str(subcategory))
            session.add(temp)

    session.commit()
    session.close()
