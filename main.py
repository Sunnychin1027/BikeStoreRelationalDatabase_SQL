"""
Question for myself:
While I was examining the data, I noticed that in the orders table, some shipped_date values are later than the required_date.
I would like to identify specific brands that may have a higher likelihood of delayed shipments.
But I also will check if the required date are later than the order date,
so we can have a better strategy for meeting customer expectations more effectively.

Here are the steps I plan to take with the data:
1. Check if all required_date values are later than the order_date.
2. Determine the number of orders and which orders have a shipped_date later than the required_date.
3. Examine the details of these orders. I believe that understanding the product and brand involved will be a valuable approach.
"""

import cursor as cursor
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# Read csv file
brands = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/brands.csv')
categories = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/categories.csv')
customers = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/customers.csv')
order_items = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/order_items.csv')
orders = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/orders.csv')
products = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/products.csv')
staffs = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/staffs.csv')
stocks = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/stocks.csv')
stores = pd.read_csv('/Users/sunnykim/Desktop/7010/M4/archive/stores.csv')

# Create database connection
connection = sqlite3.connect('bikestore_db')

# Insert data into database
brands.to_sql('brands', connection, if_exists='replace', index=False)
categories.to_sql('categories', connection, if_exists='replace', index=False)
customers.to_sql('customers', connection, if_exists='replace', index=False)
order_items.to_sql('order_items', connection, if_exists='replace', index=False)
orders.to_sql('orders', connection, if_exists='replace', index=False)
products.to_sql('products', connection, if_exists='replace', index=False)
staffs.to_sql('staffs', connection, if_exists='replace', index=False)
stocks.to_sql('stocks', connection, if_exists='replace', index=False)
stores.to_sql('stores', connection, if_exists='replace', index=False)


"""
Check if data are insert into tables
"""
# cursor = connection.cursor()
# cursor.execute('SELECT * FROM brands')
# for row in cursor.fetchall():
#     print(row)

"""
Check each category name in categories table
"""
# query = """
# SELECT category_name
# FROM categories;
# """
# df = pd.read_sql_query(query, connection)
# print(df)

# connection.close()

"""
Get each table's data information
"""
# Connect to the database
# cursor = connection.cursor()
# # Get every table name
# cursor.execute('SELECT name FROM sqlite_master WHERE type="table";')
# tables = cursor.fetchall()
# # Go through each table
# for table in tables:
#     table_name = table[0]
#     print(f"Table name: {table_name}")
#     # Get table info
#     cursor.execute(f"PRAGMA table_info({table_name});")
#     table_info = cursor.fetchall()
#     # Print table info
#     for column in table_info:
#         column_name = column[1]
#         data_type = column[2]
#         print(f" Column Name: {column_name}, Data Type: {data_type}")
# connection.close()

query1 = """
SELECT *
FROM orders 
WHERE required_date < order_date;
"""

# cursor = connection.cursor()
# cursor.execute(query1)
# rows = cursor.fetchall()
# if not rows:
#     print("All required_date values are later than order_date values.")
# else:
#     print("There are orders where required_date is not later than order_date.")
#     for row in rows:
#         print(row)
# connection.close()

query2 = """
SELECT * 
FROM orders
WHERE shipped_date > required_date;
"""
cursor = connection.cursor()
cursor.execute(query2)
rows = cursor.fetchall()
if rows:
    print("There are shipped_date values later than required_date values.")
    for row in rows:
        print(row)
    print(f"There are total {len(rows)} orders that fail expectation.")
else:
    print("There aren't orders where shipped_date is later than required_date.")

"""
Create a temporary table for query 2 so we can do further analysis.
"""

temp_table_query = """
CREATE TEMP TABLE temp_selected_orders AS
"""
temp_table_query += query2
try:
    # Execute the SQL commands as a script
    cursor.executescript(temp_table_query)
    connection.commit()
    # Verify that the temporary table has been created
    cursor.execute("SELECT * FROM temp_selected_orders LIMIT 1")
    row = cursor.fetchone()
    if row:
        print("Temporary table 'temp_selected_orders' created successfully.")
    else:
        print("Temporary table creation failed.")
except sqlite3.Error as e:
    print('An error occurred:', e)

"""
Do the JOIN to find out the product_name
"""

query3 = """
-- Join the temporary table with order_items and Production.products table
SELECT DISTINCT P.product_name
FROM temp_selected_orders AS T
JOIN order_items AS OI on T.order_id = OI.order_id
JOIN products AS P on OI.order_id = P.product_id;
"""
cursor.execute(query3)
product_names = cursor.fetchall()

for product_name in product_names:
    print(product_name[0])
print(f"There are total of {len(product_names)}  products that are usually shipped or collected late.")

"""
Now we would like to find out the products brands which it belongs to.
But first we will create a temporary table to store those products.
"""
temp_products_query = """
CREATE TEMP TABLE temp_distinct_product_names AS
SELECT DISTINCT P.product_name
FROM temp_selected_orders AS T
JOIN order_items AS OI on T.order_id = OI.order_id
JOIN products AS P on OI.order_id = P.product_id;
"""
cursor.executescript(temp_products_query)

# Now define the SQL query to retrieve brand names by joining with the "brands" table
query4 = """
-- Join the temporary table with "products" and "brands" tables to get brand names
-- Select distinct brand_name and count how many times each brand appears
SELECT B.brand_name, COUNT(*) as brand_count
FROM temp_distinct_product_names AS T
JOIN products AS P ON T.product_name = P.product_name
JOIN brands AS B ON P.brand_id = B.brand_id
GROUP BY B.brand_name;
"""
cursor.execute(query4)
results = cursor.fetchall()
print('This is the data which retrive from the late shipping, and the brand and times that are late in a duration '
      'time. ')
for row in results:
    brand_name = row[0]
    brand_count = row[1]
    print(f"Brand: {brand_name}, Count: {brand_count}")

connection.close()


