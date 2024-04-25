from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json


MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.jrr5gm
# specify a collection
collection = db.DataProject2


path = "/workspace/ds2002-dp2/data"

for (root, dirs, file) in os.walk(path):
    for f in file:
        file_path = os.path.join(root, f)
        try:
            with open(file_path) as file:
                file_data = json.load(file)
            if isinstance(file_data, list):
                collection.insert_many(file_data)  
            else:
                collection.insert_one(file_data)
        except Exception as e:
            print(f"Error in {file_path}")
            print(e)
            continue
        finally:
            print({file_path})
        