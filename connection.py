import pandas as pd
from sqlalchemy import Column, Integer, String, create_engine, false, true, ForeignKey
import psycopg2

#create connection for postgrsqldb
conn = psycopg2.connect(database="LivingLens", user='postgres', password='HugGuy#7', host='127.0.0.1', port= '5432')
conn.autocommit =True
cursor = conn.cursor()
path = "C:\\Data\\"

## fill city table with foreign keys based on country data

cursor.execute('''UPDATE city SET country_id_fk = 1 WHERE city_name='Berlin' OR
                                                                    city_name='Frankfurt' OR
                                                                    city_name= 'Münich' ''')
cursor.execute('''UPDATE city SET country_id_fk = 2 WHERE city_name='Milan' OR
                                                                    city_name='Rome' OR
                                                                    city_name= 'Venice' ''')
cursor.execute('''UPDATE city SET country_id_fk = 3 WHERE city_name='Helsinki' OR
                                                                    city_name='Lappeenranta' ''')
                                                                   



## fill subcategory table with foreign keys based on category table
berlin_df = pd.read_excel(path + "Berlin.xlsx", na_values = "nan", 
                           usecols="A:E", )

df = berlin_df.iloc[:,0]
counter = 1
for data in df:
    if str(data) == "Restaurants":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 1 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Markets":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 2 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Transportation":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 3 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Utilities (Monthly)":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 4 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Sports And Leisure":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 5 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Childcare":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 6 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Clothing And Shoes":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 7 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Rent Per Month":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 8 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Buy Apartment Price":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 9 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1
    if(str(data)) == "Salaries And Financing":
        cursor.execute('''UPDATE subcategory SET category_id_fk = 10 WHERE id = ''' + str(counter) +''' ''')
        counter = counter + 1

## fill prices table with Berlin data
df_avg_prices = berlin_df.iloc[:,2]
df_min_prices = berlin_df.iloc[:,3]
df_max_prices = berlin_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (1,
                            '''+ str(subcat_inx) +''',
                            ''' +str(data)+ ''',
                            ''' + df_min_prices[i]+''',
                            '''+df_max_prices[i]+''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1

## fill prices table with Frankfurt data
frankfurt_df = pd.read_excel(path + "Frankfurt.xlsx", na_values = "nan", 
                           usecols="A:E", )

      
df_avg_prices = frankfurt_df.iloc[:,2]
df_min_prices = frankfurt_df.iloc[:,3]
df_max_prices = frankfurt_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (2,
                            '''+ str(subcat_inx) +''',
                            ''' +str(data)+ ''',
                            ''' + df_min_prices[i]+''',
                            '''+df_max_prices[i]+''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1

## fill prices table with Münich data
munich_df = pd.read_excel(path + "Munich.xlsx", na_values = "nan", 
                           usecols="A:E", )

      
df_avg_prices = munich_df.iloc[:,2]
df_min_prices = munich_df.iloc[:,3]
df_max_prices = munich_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (3,
                            '''+ str(subcat_inx) +''',
                            ''' +str(data)+ ''',
                            ''' + df_min_prices[i]+''',
                            '''+df_max_prices[i]+''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1


## fill prices table with Rome data
rome_df = pd.read_excel(path + "Rome.xlsx", na_values = "nan", 
                           usecols="A:E", )
  
df_avg_prices = rome_df.iloc[:,2]
df_min_prices = rome_df.iloc[:,3]
df_max_prices = rome_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (4,
                            '''+ str(subcat_inx) +''',
                            ''' +str(data)+ ''',
                            ''' + df_min_prices[i]+''',
                            '''+df_max_prices[i]+''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1

## fill prices table with Venice data
venice_df = pd.read_excel(path + "Venice.xlsx", na_values = "nan", 
                           usecols="A:E", )

      
df_avg_prices = venice_df.iloc[:,2]
df_min_prices = venice_df.iloc[:,3]
df_max_prices = venice_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (5,
                            '''+ str(subcat_inx) +''',
                            ''' +str(data)+ ''',
                            ''' + df_min_prices[i]+''',
                            '''+df_max_prices[i]+''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1

## fill prices table with Milan data
milan_df = pd.read_excel(path + "Milan.xlsx", na_values = "nan", 
                           usecols="A:E", )

      
df_avg_prices = milan_df.iloc[:,2]
df_min_prices = milan_df.iloc[:,3]
df_max_prices = milan_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (6,
                            '''+ str(subcat_inx) +''',
                            ''' +str(data)+ ''',
                            ''' + df_min_prices[i]+''',
                            '''+df_max_prices[i]+''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1

## fill prices table with Helsinki data
helsinki_df = pd.read_excel(path +"Helsinki.xlsx", na_values = "nan", 
                           usecols="A:E", )

   
df_avg_prices = helsinki_df.iloc[:,2]
df_min_prices = helsinki_df.iloc[:,3]
df_max_prices = helsinki_df.iloc[:,4]

i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (7,
                            '''+ str(subcat_inx) +''',
                            ''' + str(data) + ''',
                            ''' + df_min_prices[i] +''',
                            '''+ df_max_prices[i] +''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1

## fill prices table with Lappeenranta data
lappeenranta_df = pd.read_excel(path + "Lappeenranta.xlsx", na_values = "nan", 
                           usecols="A:E", )

      
df_avg_prices = lappeenranta_df.iloc[:,2]
df_min_prices = lappeenranta_df.iloc[:,3]
df_max_prices = lappeenranta_df.iloc[:,4]


i = 0
subcat_inx = 1
for data in df_avg_prices:
    data = str(data).replace(",", "")
    df_min_prices[i] = str(df_min_prices[i]).replace(",", "")
    df_max_prices[i] = str(df_max_prices[i]).replace(",", "")
    cursor.execute('''INSERT INTO price (city_id_fk, subcategory_id_fk, average_price, min_price, max_price)
                    VALUES (8,
                            '''+ str(subcat_inx) +''',
                            ''' + str(data) + ''',
                            ''' + df_min_prices[i] +''',
                            '''+ df_max_prices[i] +''')''')
    i = i + 1
    subcat_inx = subcat_inx + 1




conn.close()