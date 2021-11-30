'''
This python code is to read JSON data from url &
then load that data to users & subscription table
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
url = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users"
# Reading the URL and getting the JSON data from spark_test_properties.py
#file using read_json function 
response = urlopen(url)
data_json = json.loads(response.read())

# This additional data frame is to read users data 
df = pd.json_normalize(data_json,sep='_',errors='ignore')

# This additional data frame is to read subscription data 
df2 = pd.json_normalize(data_json,'subscription',['id'],errors='ignore')

# datetime object containing current date and time
now = datetime.now()

# Opening the database connection
cursor = conn.cursor()

# Step for inserting data to users table
for index, row in df.iterrows():
	# Steps to calculateAge from birthDate
	f_date = datetime.strptime(row.birthDate,'%Y-%m-%dT%H:%M:%S.%f%z')
	date_time = f_date.strftime('%Y-%m-%d')
	year = date_time.split('-')[0]
	month = date_time.split('-')[1]
	day = date_time.split('-')[2]
    #calling calculateAge function from spark_test_properties module
	age_calculated = spark_test_properties.calculateAge(
                                        date(int(year), int(month), int(day)))

	# Insert Dataframe into SQL Server table: user
	try:
        #while inserting data to user table only domain is selected from email
		cursor.execute(
			"INSERT INTO dbo.users (id, city, country, emailDomain, age, "
			"isSmoking, gender, income, createdAt, updatedAt,etl_insert_dt) "
			"values(?,?,?,?,?,?,?,?,?,?,?)",
			row.id, row.city, row.country, row.email.split('@')[1],
			age_calculated, row.profile_isSmoking,
			row.profile_gender, row.profile_income, row.createdAt,
			row.updatedAt, now)
	except pyodbc.Error as e:
		print(e)
		sys.exit(1)

# Step for inserting data to subscription table    
for index, row in df2.iterrows():
	# Insert Dataframe into SQL Server table: subscription
	try:
		cursor.execute(
			"INSERT INTO dbo.subscription (createdAt, startDate, endDate,"
			" status, amount,id,etl_insert_dt) values(?,?,?,?,?,?,?)",
			row.createdAt, row.startDate, row.endDate, row.status,
			row.amount, row.id, now)
	except pyodbc.Error as e:
		print(e)
		sys.exit(1)

print('Data is loaded to table users')
print('Data is loaded to table subscription')
conn.commit()
cursor.close()
