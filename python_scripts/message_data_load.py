'''
This python code is to read JSON data from url &
then load that data to message table
'''
import pyodbc
import pandas as pd
from urllib.request import urlopen
import pandas as pd
import json
from datetime import datetime
from datetime import date
import sys
import spark_test_properties

# Getting connection details from spark_test_properties.py file
conn = spark_test_properties.connection_details()

# connect to URL & Read JSON file from that path & loading that to a dataFrame
url = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages"
response = urlopen(url)
data_json = json.loads(response.read())
df = pd.json_normalize(data_json, sep='_',errors='ignore')

# datetime object containing current date and time
now = datetime.now()

# Opening the database connection
cursor = conn.cursor()

# Insert Dataframe into SQL Server table: message
for index, row in df.iterrows():
	try:
		cursor.execute(
			"INSERT INTO dbo.message (id,senderId,receiverId,createdAt,"
			"etl_insert_dt) values(?,?,?,?,?)",
			row.id, row.senderId, row.receiverId, row.createdAt, now)
	except pyodbc.Error as e:
		print(e)
		sys.exit(1)

# Commiting the data inserts & Closing the connection
print('Data is loaded to table message')
conn.commit()
cursor.close()
