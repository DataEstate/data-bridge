from pymongo import MongoClient
import pymongo
from pytz import timezone
import pytz
import json
import time
from databridge import process_profile
import argparse

def start_mapping():
  profile_path = "profiles/atdw-mapping-accomm.json"
  ops={}
  ## If delta data is defined and not the default value (last)
  if hasattr(args, "deltaDate") and args.deltaDate != "last":
    ops={
      "srcVariables": '{"latest_update_ts":'+args.deltaDate+'}'
    }
  else:
    ## Mongo Connection required. 
    try:
      conn = MongoClient(args.host)
      db=conn[args.database]
      collection = db[args.collection]
      query={
        "type":"PRODUCT"
      }
      project={
        "_id":False,
        "update_ts":1
      }
      date_doc = collection.find_one(filter=query, projection=project, sort=[("update_ts", pymongo.DESCENDING)])
      ops={
        "srcVariables": '{"latest_update_ts":'+str(date_doc["update_ts"])+'}'
      }
    except Exception as e:
      print(e)
  print(ops)
  process_profile(profile_path, ops)

if __name__ == "__main__":
  ap = argparse.ArgumentParser(description="ATDW nightly mapper")
  ap.add_argument("-dd", "--deltaDate", default="last", help="Update timestamp (unixtimestamp) for the mapper. ")
  ap.add_argument("-db", "--database", default=None, help="Database name")
  ap.add_argument("-u", "--host", default="mongodb://localhost:27017", help="Mongo host, can be just localhost or the full host uri")
  ap.add_argument("-c", "--collection", default=None, help="Which collection to target. ")
  args = ap.parse_args()
  start_mapping()