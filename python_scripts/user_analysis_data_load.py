'''
This python code is to load data to consumption layer table user_analysis 
using users,message,subscription tables
'''
import pyodbc
import spark_test_properties
import spark_test_dervied_layer_insert_query as src_qry
import pandas as pd
import sys

# Getting connection details from spark_test_properties.py file
conn = spark_test_properties.connection_details()
# Opening the database connection
cursor = conn.cursor()


# This function is to run sql insert command 
def insert_data(query):
	# Insert Data into SQL Server table: user_analysis
	try:
		cursor.execute(query)
	except pyodbc.Error as e:
		print(e)
		sys.exit(1)

#This step is to get max dt from user table
src_dt = pd.read_sql_query(
	'''select max(etl_insert_dt) as etl_insert_dt from users;''',
                    conn)._get_value(0, 'etl_insert_dt')
#This step is to get max dt from user_analysis table
tgt_dt = pd.read_sql_query(
	'''select max(etl_insert_dt) as etl_insert_dt from user_analysis;''',
                    conn)._get_value(0, 'etl_insert_dt')

# The below condition is to load only recent data or historical data load
#This filter is to check if any data available in source table
if src_dt is not None:
    #This filter is to check if it is full load/historical data load
	if tgt_dt is not None:
        #this filter is to load only recent inserts from source table
		if src_dt > tgt_dt:
            #Where condition is appened to sql insert query to load only delta records
			query = src_qry.user_analysis_insert_qry + \
					' where u.etl_insert_dt > ' + "'" + str(
				tgt_dt)[0:23] + "'"
			# Calling insert_data function to insert data to user_analysis table
			insert_data(query)
			print('Delta Data Insertion is completed')
		else:
			print('No latest data to insert')
	else:
		# Calling insert_data function to insert data to user_analysis table
		insert_data(src_qry.user_analysis_insert_qry)
		print('Full Load/Historical Data Insertion is completed')
else:
	print('No data to insert')

# Commiting the data inserts & Closing the connection
conn.commit()
cursor.close()
