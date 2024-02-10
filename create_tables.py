from array import array
from decimal import Decimal
import itertools
from operator import index
import pandas as pd
from tokenize import Double, Number, String
import psycopg2
import sqlalchemy
from sqlalchemy import Column, Integer, String, create_engine, false, true, ForeignKey, Double, URL
from sqlalchemy.orm import declarative_base, sessionmaker


# Connection
url_object = URL.create(
    "postgresql",
    username="postgres",
    password="HugGuy#7",  # plain (unescaped) text
    host="localhost",
    database="LivingLens",
)

Base = declarative_base()
engine = create_engine(url_object)

path = "C:\\Data\\"
# Lifestlye
class Lifestyle(Base):
    __tablename__ = "lifestyle"

    id = Column(Integer, primary_key=True, autoincrement=True,
                nullable=False, unique=true)
    lifestyle_name = Column("lifestyle_category_name", String(100))

def __init__(self, name):
    self.lifestyle_name = name

Base.metadata.create_all(bind = engine)
Session = sessionmaker(bind = engine)
session = Session()
session.commit()
session.close()

#Country
class Country(Base):
    __tablename__ = "country"

    id = Column(Integer, primary_key=True, autoincrement=True,
                nullable=False, unique=true)
    country_name = Column("country_name", String(100))

def __init__(self, name):
    self.country_name = name

Base.metadata.create_all(bind = engine)
Session = sessionmaker(bind = engine)
session = Session()
session.commit()
session.close()

# City
class City(Base):
    __tablename__ = "city"

    id = Column(Integer, primary_key=True, autoincrement=True,
                nullable=False, unique=true)
    city_name = Column("city_name", String(100))
    country_id_fk = Column(Integer, ForeignKey("country.id", onupdate="CASCADE"), nullable = True)


def __init__(self, name):
    self.city_name = name

Base.metadata.create_all(bind = engine)
Session = sessionmaker(bind = engine)
session = Session()
session.commit()
session.close()

# Category
class Category(Base):
    __tablename__ = 'category'

    id = Column(Integer, primary_key=True, autoincrement=True,
                nullable=False, unique=true)
    category_name = Column("category_name", String(50))

def __init__ (self, name):
    self.category_name = name

Base.metadata.create_all(bind = engine)
Session = sessionmaker(bind = engine)
session = Session()
session.commit()
session.close()

class SubCategory(Base):
    __tablename__ = "subcategory"

    id = Column(Integer, primary_key=True, autoincrement=True,
                nullable=False, unique=True)
    subcategory_name = Column("subcategory_name", String(100))
    category_id_fk = Column(Integer, ForeignKey("category.id", onupdate="CASCADE"), nullable = True)

def __init__ (self,name):
    self.subcategory_name = name

Base.metadata.create_all(bind = engine)
Session = sessionmaker(bind = engine)
session = Session()
session.commit()
session.close()

class Price(Base):
    __tablename__ = "price"

    id = Column(Integer,primary_key=True,autoincrement=True, nullable=False, unique=true)
    city_id_fk = Column(Integer, ForeignKey("city.id", onupdate="CASCADE"), nullable=True)
    subcategory_id_fk = Column(Integer, ForeignKey("subcategory.id", onupdate="CASCADE"), nullable = True)
    average_price = Column("average_price", Double(8,2))
    min_price = Column("min_price", Double(8,2))
    max_price = Column("max_price", Double(8,2))

def __init__(self, city_id,subcategory_id ):
    self.city_id_fk = city_id
    self.subcategory_id_fk = subcategory_id

Base.metadata.create_all(bind = engine)
Session = sessionmaker(bind = engine)
session = Session()
session.commit()

base_df = pd.read_excel(path + "Lifestyle.xlsx", na_values = "nan",
                           usecols="A:E")

# Load countries' data      
countries = base_df.iloc[:,0]
for country in countries:
    if str(country) != 'nan':
        temp = Country(country_name=str(country))
        session.add(temp)

# Load cities' data   
cities = base_df.iloc[:,1]
for city in cities:
    if str(city) != 'nan':
        temp = City(city_name=str(city))
        session.add(temp)

# Load lifestyles' data   
lifestyles = base_df.iloc[:,2]
for lifestyle in lifestyles:
    if str(lifestyle) != 'nan':
        temp = Lifestyle(lifestyle_name=str(lifestyle))
        session.add(temp)

# Load categories' data   
categories = base_df.iloc[:,3]
for category in categories:
    if str(category) != 'nan':
        temp = Category(category_name=str(category))
        session.add(temp)

# Load subcategories' data   
subcategories = base_df.iloc[:,4]
for subcategory in subcategories:
    if str(subcategory) != 'nan':
        temp = SubCategory(subcategory_name=str(subcategory))
        session.add(temp)

session.commit()
session.close()