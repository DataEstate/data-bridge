# NATIVE Modules
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
from collections import defaultdict
import json
import time
import csv
from math import ceil
import sys
import argparse
from argparse import RawTextHelpFormatter
import requests
# External Modules
from pymongo import MongoClient
import pymongo
from pytz import timezone
import pytz
import pyodbc

## VERSION 0.1.0 BETA
# Data Bridge BETA. Limited Support. 

conf={}
dest_conn=None
sql_table_flag=0

def import_data():
	global conf
	if args.profile is not None:
		with open (args.profile, 'r', encoding='utf-8') as f:
			conf=json.load(f)
		## TODO
	## switch for src
	src_conf = conf["source"]
	src_type = src_conf.get("type", "MONGO")
	dest_conf = conf["dest"]
	dest_type = dest_conf.get("type", "PRINT")
	if src_type == "CSV": ## defaults to Mongo
		process_row = get_process(dest_type)
		src=parse_csv(src_conf["file_path"]) ## List
		### DO SOMETHING HERE
	elif src_type == "API":
		process_row = get_process(dest_type)
		iterate_api(connection=src_conf["connection"], query=src_conf["query"], process_func=process_row)
	elif src_type=="MSSQL":
		process_row = get_process(dest_type)
		iterate_mssql(connection=src_conf["connection"], query=src_conf["query"], process_func=process_row)
		##TODO: set to another function
	elif src_type=="MONGO":
		process_row = get_process(dest_type)
		iterate_mongo(connection=src_conf["connection"], query=src_conf["query"], process_func=process_row)

##### SOURCE PARSING #####
def iterate_mssql(connection, query, process_func):
	conn_string = 'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'.format(**connection)
	global dest_conn
	with pyodbc.connect(conn_string) as conn:
		cursor=conn.cursor()
		rows=cursor.execute(query)
		headings=[column[0] for column in cursor.description]
		row=rows.fetchone()
		while row is not None:
			item = OrderedDict(zip(headings, row))
			## Data Insert
			process_func(item)
			row=rows.fetchone()
		cursor.close()
		if dest_conn is not None:
			dest_conn.close()
			print("Connection closed")

def iterate_mongo(connection, query, process_func):
	conn = MongoClient(host=connection.get("server", "localhost"), port=connection.get("port", 27017))
	db = conn[connection["database"]]
	collection=db[query["collection"]]
	## Use find if exists. When aggregating, make sure find is not set. 
	if query.get("find", None) is not None:
		print(query["find"])
		rows = collection.find(query["find"], limit=query.get("limit", 0), skip=query.get("skip", 0), sort=query.get("sort", None))
	elif query.get("aggregate", None) is not None:
		pipeline = query["aggregate"]
		if query.get("limit", -1) > 0:
			pipeline.append({"$limit": int(query["limit"])})
		if query.get("skip", -1) > 0:
			pipeline.append({"$skip": int(query["skip"])})
		print(pipeline)
		rows = collection.aggregate(pipeline)
	for row in rows:
		item = OrderedDict(row)
		process_func(item)
	if dest_conn is not None:
		dest_conn.close()
		print("Connection closed")
	conn.close()

def iterate_api(connection, query, process_func):
	method = connection.get("method", "get")
	api_url = connection["server"]+(":"+connection["port"] if connection.get("port", None) is not None else "")+(connection["endpoint"] if connection.get("endpoint", None) is not None else "")
	params = query.get("params", None)
	data = query.get("data", None)
	headers = None
	if connection.get("authentication", None) is not None:
		conn_type = connection["authentication"].get("type", "basic")
		if conn_type == "bearer":
			headers = {
				"Authorization": "Bearer "+connection["authentication"]["token"]
			}
	r = requests.request(method=method, url=api_url, params=params, data=data, headers=headers)
	if r.status_code == 200:
		rows=r.json() ## Assume JSON at the moment
		if query.get("response_path", None) is not None:
			rows=get_child_element(query["response_path"], parent_element=rows)
		for row in rows:
			item = OrderedDict(row)
			process_func(item)
		if dest_conn is not None:
			dest_conn.close()


def process_mssql(item):
	global dest_conn
	global conf
	connection = conf["dest"]["connection"]
	global sql_table_flag
	if dest_conn == None:
		conn_string = 'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'.format(**connection)
		dest_conn = pyodbc.connect(conn_string)
	cursor=dest_conn.cursor()
	if sql_table_flag == 0 and conf["dest"].get("create", None) is not None:
		## Execute table create operations
		table_create = create_query(conf["dest"]["create"])
		# print(table_create)
		cursor.execute(table_create)
		dest_conn.commit()
		sql_table_flag = 1
	## Execute Query
	query=conf["dest"]["query"].format(**item)
	# print(query)
	cursor.execute(query)
	dest_conn.commit()
	cursor.close()

def process_mongo(item):
	global dest_conn
	global conf
	connection = conf["dest"]["connection"]
	if dest_conn == None:
		dest_conn = MongoClient(host=connection.get("server", "localhost"), port=connection.get("port", 27017))
		print("Connect once")
	db = dest_conn[connection["database"]]
	query=conf["dest"]["query"]
	find_doc=append_variables(query["find"], item)
	print(find_doc)
	update_doc=append_variables(query["update"], item)
	print(update_doc)
	## Always append update time
	if update_doc.get("$set", None) is None:
		update_doc["$set"] = {}
	update_doc["$set"]["update_date"] = datetime.utcnow()
	## Insert
	# result=db[query["collection"]].find_one(find_doc)
	update=db[query["collection"]].update_one(find_doc, update_doc, upsert=args.upsert if args.upsert is not None else False)

def append_variables(mongo_doc, src_dict):
	updated_doc = {}
	if isinstance(mongo_doc, list):
		updated_doc = []
		for v in mongo_doc:
			if isinstance(v, str):
				default_data=dict_to_default(src_dict)
				updated_doc.append(v.format_map(defaultdict(str, default_data)))
			elif isinstance(v, dict) or isinstance(v, list):
				updated_doc.append(append_variables(v, src_dict))
			else:
				updated_doc.append(v)
	else:
		for (key,v) in mongo_doc.items():
			if isinstance(v, str):
				default_data=dict_to_default(src_dict)
				updated_doc[key] = v.format_map(defaultdict(str, default_data))
			elif isinstance(v, dict) or isinstance(v, list):
				updated_doc[key] = append_variables(v, src_dict)
			else:
				updated_doc[key] = v
	return updated_doc

def process_csv(item):
	global dest_conn
	global conf
	reserved={
		"today":datetime.now().strftime("%Y%m%d")
	}
	file_path = conf["dest"]["file_path"].format(**reserved)
	query = conf["dest"]["query"]
	headers = query.get("headers", "").split(",")
	if dest_conn == None:
		dest_conn = open(file_path, 'w', encoding='utf-8-sig')
		csvwriter=csv.DictWriter(dest_conn, delimiter=',', fieldnames=headers)
		csvwriter.writeheader()
		print("Write once")
	if 'csvwriter' not in locals():
		csvwriter=csv.DictWriter(dest_conn, delimiter=',', fieldnames=headers)
	insert_doc=append_variables(query["body"], item)
	csvwriter.writerow(insert_doc)

def process_api(item):
	print(item)
	print("done")

## Helpers
def get_child_element(json_path="", parent_element={}):
	paths=json_path.split(".")
	#TODO array support
	current_element = parent_element
	for path in paths:
		current_element = parent_element.get(path, current_element)
	return current_element

def dict_to_default(dict_data):
	new_dict=defaultdict(str, dict_data)
	for k,v in dict_data.items():
		if isinstance(v, dict):
			new_dict[k]=dict_to_default(v)
	return new_dict


def create_query(create_options):
	# start with existence
	table_name = create_options["name"]
	query_list = []
	## check for existence
	query_list.append("IF EXISTS (SELECT * FROM sysobjects WHERE name='"+table_name+"' and xtype = 'U') BEGIN") #U for user table
	if create_options.get("exist_action", "") == "DROP":
		query_list.append("DROP TABLE "+table_name)
		query_list.append("END")
	else:
		query_list.append("print('exists')")
		query_list.append("END")
		query_list.append("ELSE BEGIN")

	table_fields = []
	query_list.append("CREATE TABLE ")
	query_list.append(table_name)
	query_list.append(" (")
	
	for (field,value) in create_options["properties"].items():
		tf_string = [field, value["type"]]
		if value.get("identity", None) is not None:
			tf_string.append("IDENTITY"+value["identity"])
		if value.get("primary_key", False) == True:
			tf_string.append("PRIMARY KEY")
		if value.get("nullable", True) == False:
			tf_string.append("NOT NULL")
		table_fields.append(" ".join(tf_string))
	query_list.append(", ".join(table_fields))
	query_list.append(");")
	query_list.append("END")
	return " ".join(query_list)

def get_process(dest_type="CSV"):
	if dest_type=="MONGO":
		return process_mongo
	elif dest_type=="MSSQL":
		return process_mssql
	elif dest_type=="CSV":
		return process_csv
	elif dest_type=="API":
		return process_api
	else:
		return print
## Parse CSV
def parse_csv(file_path=""):
	with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
		return list(csv.DictReader(csvfile, delimiter=','))
	return None

if __name__ == "__main__":
	ap = argparse.ArgumentParser(description=
		"***********     Data Estate Data Bridge (v0.1.0 BETA)    ***********\n"\
		"  Data Bridging App to sync data between different\n"\
		"  databases. Currently only MS-SQL to MongoDB\n"\
		"  Usage: python3 data-bridge.py [-options] [source] [destination]", formatter_class=RawTextHelpFormatter)
	# ap = add_argument("source", metavar="src_conf", help="R:REQUIRED: Source configuration file. ")
	# ap = add_argument("dest", metavar="dest_conf", help="R:REQUIRED: Destination configuration file. ")
	ap.add_argument("-st", "--sourceType", default="MONGO", help="Source type for the connection. Options are 'MONGO', 'SQL', 'CSV'")
	ap.add_argument("--profile", default=None, help="Set this up using the configuration file. ")
	ap.add_argument("-q", "--query", default="", help="Query string. If it's a MongoDB or SQL")
	ap.add_argument("--upsert", action="store_true", help="Should create the listing if it doesn't exist. Use with caution")
	args = ap.parse_args()
	import_data()


