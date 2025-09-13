# NATIVE Modules
from datetime import datetime, UTC, timedelta
from collections import OrderedDict
from collections import defaultdict
import json
import time
import csv
from math import ceil
from math import floor
import sys
import argparse
from argparse import RawTextHelpFormatter
import requests
# External Modules
from pymongo import MongoClient, InsertOne, DeleteMany, ReplaceOne, UpdateOne
import pymongo
from pytz import timezone
import pytz
import pyodbc

import traceback

## VERSION 0.2.0 BETA
# Data Bridge BETA. Limited Support.
conf={}
log_paths={}
row_count=0
update_count=0
error_count=0
dest_conn=None
log_doc={}
sql_table_flag=0
bulkOps=[]
batch_size = 3000

tf='%Y-%m-%d'
df='%Y-%m-%d %H:%M:%s'
st=datetime.now(UTC)

process_log_key="process"
error_log_key="error"
## MAIN PROCESS ##
# Basic flow of this application is by running the iterate_{data_source} based on the profile,
# The iterate_{datasource} functions will query the datasource, get the result, iterate through
# each row/item and then call the process_{datasource} functions to do the insert.
#
# There is currently no support for bulk run methods.

def import_data():
	global conf
	global log_paths
	if args.profile is not None:
		process_profile(args.profile)

def process_profile(profile_path, ops=None):
	global conf
	global log_paths
	global log_doc
	global args
	with open(profile_path, 'r', encoding='utf-8') as f:
		conf=json.load(f)
		## Check if log paths exists
		if conf.get("logs", None) is not None:
			for (key,logConf) in conf["logs"].items():
				if logConf.get("path", None) is not None:
					logConf["path"] = logConf["path"]+"-"+datetime.now(UTC).strftime('%Y%m%d')+'.log'
					# Only add if it has path.
					log_paths[key] = logConf
	merge_args(ops)
	## switch for src
	src_conf = conf["source"]
	src_type = src_conf.get("type", "MONGO")
	dest_conf = conf["dest"]
	dest_type = dest_conf.get("type", "PRINT")
	## LOG
	log_process_if_exists("*******************\nSync started. ", process_log_key)
	log_process_if_exists(" Source type: "+src_type, process_log_key)
	log_doc["type"] = "DataBridge"
	log_doc["src_type"] = src_type
	log_doc["dest_type"] = dest_type
	log_doc["start"] = datetime.now(UTC) ## for mongoDB
	log_doc["logs"] = []
	log_doc["runs"] = []

	if src_type == "CSV": ## defaults to Mongo
		process_row = get_process(dest_type)
		src=parse_csv(src_conf["file_path"]) ## List
		### DO SOMETHING HERE
	elif src_type == "API":
		process_row = get_process(dest_type)
		try:
			iterate_api(connection=src_conf["connection"], query=src_conf["query"], process_func=process_row)
		except Exception as e:
			log_process_if_exists(" Exception in iterate_api: "+str(e), error_log_key)
	elif src_type=="MSSQL":
		process_row = get_process(dest_type)
		try:
			iterate_mssql(connection=src_conf["connection"], query=src_conf["query"], process_func=process_row)
		except Exception as e:
			log_process_if_exists(" Exception in iterate_mssql: "+str(e), error_log_key)
		##TODO: set to another function
	elif src_type=="MONGO":
		process_row = get_process(dest_type)
		try:
			iterate_mongo(connection=src_conf["connection"], query=src_conf["query"], process_func=process_row)
		except Exception as e:
			log_process_if_exists(" Exception in iterate_mongo: "+str(e), error_log_key)
			traceback.print_exc()
##### SOURCE PARSING #####
def iterate_mssql(connection, query, process_func):
	conn_string = 'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'.format(**connection)
	global dest_conn
	global row_count
	row_count=0
	with pyodbc.connect(conn_string) as conn:
		cursor=conn.cursor()
		rows=cursor.execute(query)
		headings=[column[0] for column in cursor.description]
		row=rows.fetchone()
		while row is not None:
			row_count = row_count + 1
			item = OrderedDict(zip(headings, row))
			## Data Insert
			process_func(item)
			row=rows.fetchone()
		cursor.close()
		## SQL count is done afterwards.
		log_process_if_exists(" Source row processed (SQL): "+str(row_count))
		if dest_conn is not None:
			dest_conn.close()
			print("Connection closed")
		et=datetime.now(UTC)
		log_process_if_exists("Sync Complete: \n  Total: "+str(row_count)+"\n  Updated: "+str(update_count)+"\n  Skipped: "+str(error_count)+"\n  Sync Time: "+str(et-st)+"\n*******************")

def iterate_mongo(connection, query, process_func):
	conn = MongoClient(host=connection.get("server", "localhost"), port=connection.get("port", 27017))
	db = conn[connection["database"]]
	collection=db[query["collection"]]
	## Use find if exists. When aggregating, make sure find is not set.
	global row_count
	global update_count
	global error_count
	global bulkOps
	global dest_conn
	global log_doc
	global log_paths
	global conf
	bulkOps=[]
	row_count = 0
	if query.get("find", None) is not None:
		# print(query["find"])
		srcQuery = query["find"]
		if args.srcVariables is not None:
			externalVars=json.loads(args.srcVariables)
			log_process_if_exists("  Src Variables Found: "+args.srcVariables, process_log_key)
			srcQuery = append_variables(srcQuery, externalVars)
		log_process_if_exists("Source Query: "+str(srcQuery), process_log_key)
		rows = collection.find(srcQuery, limit=query.get("limit", 0), skip=query.get("skip", 0), sort=query.get("sort", None))

	elif query.get("aggregate", None) is not None:
		pipeline = query["aggregate"]
		# check if there're any external variables, if so. use append variables to add them.
		if args.srcVariables is not None:
			externalVars=json.loads(args.srcVariables)
		#	print(externalVars)
			log_process_if_exists("  Src Variables Found: "+args.srcVariables, process_log_key)
			pipeline = append_variables(pipeline, externalVars)
		#	print(pipeline)
		if query.get("limit", -1) > 0:
			pipeline.append({"$limit": int(query["limit"])})
		if query.get("skip", -1) > 0:
			pipeline.append({"$skip": int(query["skip"])})
		log_process_if_exists("  Source Query: "+str(pipeline), process_log_key)
		rows = collection.aggregate(pipeline)
	for row in rows:
		row_count = row_count + 1
		item = OrderedDict(row)
		process_func(item)
	## Check bulk options. If there are bulk options to perform. execute.
	process_bulk()

	log_process_if_exists(" Processed count: "+str(row_count))
	if dest_conn is not None:
		dest_conn.close()
		print("Connection closed")
	conn.close()
	et=datetime.now(UTC)
	log_process_if_exists("Sync Complete: \n  Total: "+str(row_count)+"\n  Updated: "+str(update_count)+"\n  Skipped: "+str(error_count)+"\n  Sync Time: "+str(et-st)+"\n*******************")
	log_doc["end"] = datetime.now(UTC)
	## If DB
	if log_paths.get(process_log_key, None) is not None:
		if log_paths[process_log_key].get("db_log", None) is not None:
			with MongoClient(host=log_paths[process_log_key]["db_log"]["server"], port=log_paths[process_log_key]["db_log"]["port"]) as log_conn:
				log_db = log_conn[log_paths[process_log_key]["db_log"]["database"]]
				log_collection = log_db[log_paths[process_log_key]["db_log"]["collection"]]
				log_collection.insert_one(log_doc)

	## Post scripts if any
	if conf.get("post_scripts", None) is not None:
		post_scripts = conf["post_scripts"]
		for post_script in post_scripts:
			if post_script.get("type", "MONGO") == "MONGO":
				with MongoClient(host=post_script["connection"].get("server", "localhost"), port=post_script["connection"].get("port", 27017)) as script_conn:
					script_db = script_conn[post_script["connection"]["database"]]
					if post_script.get("query", None) is not None:
						script_query = post_script["query"]
						script_collection = script_db[script_query["collection"]]
						if script_query.get("update", None) is not None: ## it's an update
							if script_query.get("multi", False) == True:
								update_doc = script_collection.update_many(script_query["find"], script_query["update"])
							else:
								update_doc = script_collection.update_one(script_query["find"], script_query["update"])

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
		log_process_if_exists(" Processed count: "+str(rows.count))
		for row in rows:
			item = OrderedDict(row)
			process_func(item)
		if dest_conn is not None:
			dest_conn.close()

def process_mssql_row(item):
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

def process_mongo_row(item):
	global dest_conn
	global conf
	global row_count
	global error_count
	global log_paths
	global log_doc
	global bulkOps

	connection = conf["dest"]["connection"]
	if dest_conn == None:
		dest_conn = MongoClient(host=connection.get("server", "localhost"), port=connection.get("port", 27017))
	# db = dest_conn[connection["database"]]
	query=conf["dest"]["query"]
	find_doc=append_variables(query["find"], item)

	## Determine whether this is an update or insert
	if query.get("update", None) is not None:
		update_doc=append_variables(query["update"], item)
		## Always append update time IF current date is not set. 
		if update_doc.get("$set", None) is None:
			update_doc["$set"] = {}
		if update_doc.get("$currentDate", None) is None or update_doc["$currentDate"].get("update_date", None) is None:
			update_doc["$set"]["update_date"] = datetime.now(UTC)
		## Insert
		
		bulk_batch_index= floor(row_count / batch_size)
		## If current batch at bulk_batch_index is empty, initialise a new list
		if bulk_batch_index >= len(bulkOps):
			bulkOps.append([])
			if log_paths.get(process_log_key, None) is not None:
				log_process_if_exists("Bulk Batch: "+str(bulk_batch_index))
		try:
			should_upsert = args.upsert if hasattr(args, "upsert") else True
			bulkOps[bulk_batch_index].append(UpdateOne(find_doc, update_doc, upsert=should_upsert))
			log_doc["logs"].append(find_doc)
			# bulkOps[bulk_batch].append(UpdateOne(find_doc, update_doc, upsert=args.upsert if hasattr(args, "upsert") else True))
			# update=db[query["collection"]].update_one(find_doc, update_doc, upsert=args.upsert if args.upsert is not None else False)
			# log_process_if_exists(" Found "+str(update.matched_count)+" item and updated "+str(update.modified_count)+" item. ")
			# update_count = update_count + update.modified_count
			if log_paths.get(process_log_key, None) is not None:
				if log_paths[process_log_key].get("iterate_row_format", None) is not None:
					log_process_if_exists(" Custom log: "+log_paths[process_log_key]["iterate_row_format"].format_map(item))
		except Exception as e:
			log_process_if_exists("Error occured when processing Mongo row: "+str(row_count)+".\nException: "+str(e), error_log_key)
			error_count = error_count + 1
	## If no "update command", insert directly
	else:
		bulk_batch_index= floor(row_count / batch_size)
		## If current batch at bulk_batch_index is empty, initialise a new list
		bulkOps.append([])
		# try:
		bulkOps[bulk_batch_index].append(InsertOne(item))
		if log_paths.get(process_log_key, None) is not None:
			if log_paths[process_log_key].get("iterate_row_format", None) is not None:
				log_process_if_exists(" Custom log: "+log_paths[process_log_key]["iterate_row_format"].format_map(item))
		# except Exception as e:
		# 	log_process_if_exists("Error occured when processing Mongo row: "+str(row_count)+".\nException: "+str(e), error_log_key)
		# 	error_count = error_count + 1

# For MongoDB only.
def append_variables(mongo_doc, src_dict):
	global args
	updated_doc = {}
	# if list
	if isinstance(mongo_doc, list):
		updated_doc = []
		for v in mongo_doc:
			# If v has value or process is to include blanks, then insert, else ignore.
			if v or not args.ignoreBlank:
				if isinstance(v, str):
					default_data=dict_to_default(src_dict)
					updated_doc.append(v.format_map(defaultdict(str, default_data)))
				elif isinstance(v, dict) or isinstance(v, list):
					updated_doc.append(append_variables(v, src_dict))
				else:
					updated_doc.append(v)
	# else dictionary
	else:
		for (key,v) in mongo_doc.items():
			# If v has value or process is to include blanks, then insert, else ignore.
			if v or not args.ignoreBlank:
				# If string, then v could be a path. Parse it.
				if isinstance(v, str):
					## Used in Mongo Only. This indicates to system to match exact object.
					# Date String.
					if v.startswith("_$d:"): ##Date from date string
						date_value = get_child_element(v[4:], src_dict)
						#print(date_value)
						src_value = datetime.strptime(date_value, tf)
						#print(src_value)
					elif v.startswith("_$t:"): #Date from timestamp string
						date_value = get_child_element(v[4:], src_dict)
						#print(date_value)
						src_value = datetime.fromtimestamp(date_value)
						#print(src_value)
					elif v.startswith("_$i:"): #integer from string.
						date_value = get_child_element(v[4:], src_dict)
						#print(date_value)
						src_value = int(date_value)
						#print(src_value)
					elif v.startswith("_$"):
						src_value = get_child_element(v[2:], src_dict)
					else:
						default_data=dict_to_default(src_dict)
						src_value =  v.format_map(defaultdict(str, default_data))

					# if the value is not empty
					if src_value or not args.ignoreBlank:
						updated_doc[key] = src_value
				elif isinstance(v, dict) or isinstance(v, list):
					updated_doc[key] = append_variables(v, src_dict)
				else:
					updated_doc[key] = v
	return updated_doc

def process_csv_row(item):
	global dest_conn
	global conf
	global row_count
	global update_count
	global error_count
	reserved={
		"today":datetime.now(UTC).strftime("%Y%m%d")
	}
	file_path = conf["dest"]["file_path"].format(**reserved)
	query = conf["dest"]["query"]
	headers = query.get("headers", "").split(",")
	if dest_conn == None:
		dest_conn = open(file_path, 'w', encoding='utf-8-sig')
		csvwriter=csv.DictWriter(dest_conn, delimiter=',', fieldnames=headers)
		csvwriter.writeheader()
	if 'csvwriter' not in locals():
		csvwriter=csv.DictWriter(dest_conn, delimiter=',', fieldnames=headers)
	insert_doc=append_variables(query["body"], item)
	try:
		csvwriter.writerow(insert_doc)
		update_count = update_count + 1
		if log_paths.get(process_log_key, None) is not None:
			if log_paths.get("iterate_row_format", None) is not None:
				log_process_if_exists(" Custom log: "+log_paths[process_log_key]["iterate_row_format"].format_map(item))
	except Exception as e:
		log_process_if_exists("Error occured when processing CSV row: "+str(row_count)+".\nException: "+str(e))
		error_count = error_count + 1

def process_api_row(item):
	print(item)
	print("done")

## Currently used for any destination that supports bulk operations
def process_bulk():
	global conf
	global bulkOps
	global dest_conn
	global update_count
	global log_doc
	print("Processing bulk actions")
	print("Bulk Ops length: " + str(len(bulkOps)))
	query=conf["dest"]["query"]
	if conf["dest"].get("type", "MONGO") == "MONGO" and len(bulkOps) > 0:
		connection = conf["dest"]["connection"]
		if dest_conn == None:
			dest_conn = MongoClient(host=connection.get("server", "localhost"), port=connection.get("port", 27017))
		db = dest_conn[connection["database"]]
		try:
			for bulk_batch in bulkOps:
				updates=db[query["collection"]].bulk_write(bulk_batch)
				bulkMessage = " Bulk write occured:\n Inserted: "+str(updates.inserted_count)+"\n Matched: "+str(updates.matched_count)+"\n  Modified: "+str(updates.modified_count)+"\n  Upserted: "+str(updates.upserted_count)
				log_doc["runs"].append({
					"found": updates.matched_count,
					"updated": updates.modified_count,
					"upserted": updates.upserted_count
				})
				log_process_if_exists(bulkMessage, process_log_key)
				print(bulkMessage)
				update_count = updates.matched_count
		except Exception as e:
			log_process_if_exists(" Error occured when running bulk write event. \n  Exception: "+str(e), error_log_key)
			print("Error occured")
			print(e)
			#print(bulkOps[:10])
			traceback.print_exc()
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
		return process_mongo_row
	elif dest_type=="MSSQL":
		return process_mssql_row
	elif dest_type=="CSV":
		return process_csv_row
	elif dest_type=="API":
		return process_api_row
	else:
		return print
## Parse CSV
def parse_csv(file_path=""):
	with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
		return list(csv.DictReader(csvfile, delimiter=','))
	return None

def merge_args(ops=None):
	global args
	if ops is not None:
		# set ops default
		ops["ignoreBlank"] = ops["ignoreBlank"] if ops.get("ignoreBlank", None) is not None else False
		ops["upsert"] = ops["upsert"] if ops.get("upsert", None) is not None else False
		args = argparse.Namespace(**ops)

def log_process_if_exists(message="", log_key="process"):
	global log_paths
	global dest_conn
	if log_paths.get(log_key, None) is not None:
		log_process(log_paths[log_key]["path"], message)
	else:
		print(log_key + ": "+message)
## Logging
def log_process(file_path="", message=""):
	with open(file_path, "a+") as data_log:
		try:
			indent="    "
			data_log.write("\n"+datetime.now(UTC).strftime(df)+":")
			data_log.write(indent+message)
		except Exception as e:
			print("Log error occured")
			print(e)

if __name__ == "__main__":
	global args
	ap = argparse.ArgumentParser(description=
		"***********     Data Estate Data Bridge (v0.2.0 BETA)    ***********\n"\
		"  Data Bridging App to sync data between different\n"\
		"  databases. Currently only MS-SQL to MongoDB. \n\n"\
		"  Check website for latest updates and usage. \n"\
		"  Usage: python3 data-bridge.py [-options] [source] [destination]\n", formatter_class=RawTextHelpFormatter)
	# ap = add_argument("source", metavar="src_conf", help="R:REQUIRED: Source configuration file. ")
	# ap = add_argument("dest", metavar="dest_conf", help="R:REQUIRED: Destination configuration file. ")
	ap.add_argument("-st", "--sourceType", default="MONGO", help="Source type for the connection. Options are 'MONGO', 'SQL', 'CSV'")
	ap.add_argument("--profile", default=None, help="Set this up using the configuration file. ")
	ap.add_argument("-q", "--query", default="", help="Query string. If it's a MongoDB or SQL")
	ap.add_argument("--upsert", action="store_true", help="Should create the listing if it doesn't exist. Use with caution")
	ap.add_argument("--ignoreBlank", default=False, help="When processing imports, ignore null values. Used for flexible schema like MongoDB only. ")
	ap.add_argument("-sv", "--srcVariables", default=None, help="Optional JSON dictionary of external variables to pass into the process. ")
	args = ap.parse_args()
	# ops = vars(args) ## Set is as dicionary
	import_data()

