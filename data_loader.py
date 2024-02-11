import pandas as pd

from database import create_session
from models import Lifestyle, Country, City, Category, SubCategory


def load_lifestyle_data():
    path = "data_files/lifestyle_data/Lifestyle.xlsx"
    base_df = pd.read_excel(path, na_values="nan", usecols="A:E")

    # Create session
    session = create_session()

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
    session.query(City).filter(City.city_name.in_(['Berlin', 'Frankfurt', 'Münich'])).update(
        {City.country_id_fk: 1}, synchronize_session=False)
    session.query(City).filter(City.city_name.in_(['Milan', 'Rome', 'Venice'])).update({City.country_id_fk: 2},
                                                                                       synchronize_session=False)
    session.query(City).filter(City.city_name.in_(['Helsinki', 'Lappeenranta'])).update({City.country_id_fk: 3},
                                                                                        synchronize_session=False)

    session.commit()
    session.close()
