from sqlalchemy import Column, Integer, String, ForeignKey, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Lifestyle(Base):
    __tablename__ = "lifestyle"

    id = Column(Integer, primary_key=True)
    lifestyle_name = Column(String)

    def __init__(self, name):
        self.lifestyle_name = name


class Country(Base):
    __tablename__ = "country"

    id = Column(Integer, primary_key=True)
    country_name = Column(String)

    def __init__(self, name):
        self.country_name = name


class City(Base):
    __tablename__ = "city"

    id = Column(Integer, primary_key=True)
    city_name = Column(String)
    country_id_fk = Column(Integer, ForeignKey("country.id"))

    def __init__(self, name):
        self.city_name = name


class Category(Base):
    __tablename__ = 'category'

    id = Column(Integer, primary_key=True)
    category_name = Column(String)

    def __init__(self, name):
        self.category_name = name


class SubCategory(Base):
    __tablename__ = "subcategory"

    id = Column(Integer, primary_key=True)
    subcategory_name = Column(String)
    category_id_fk = Column(Integer, ForeignKey("category.id"))

    def __init__(self, name):
        self.subcategory_name = name


class Price(Base):
    __tablename__ = "price"

    id = Column(Integer, primary_key=True)
    city_id_fk = Column(Integer, ForeignKey("city.id"))
    subcategory_id_fk = Column(Integer, ForeignKey("subcategory.id"))
    average_price = Column(Float)
    min_price = Column(Float)
    max_price = Column(Float)

    def __init__(self, city_id, subcategory_id, average_price, min_price, max_price):
        self.city_id_fk = city_id
        self.subcategory_id_fk = subcategory_id
        self.average_price = average_price
        self.min_price = min_price
        self.max_price = max_price
