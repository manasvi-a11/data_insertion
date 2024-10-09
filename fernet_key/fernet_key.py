import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus
from cryptography.fernet import Fernet

key = Fernet.generate_key()

try:
    mongo_end_point = "ip-15-100-128-214.ap-south-1.compute.internal:27017"
    mongo_user = "nextenti_admin"
    mongo_pass_word = "NextEntiData_2023"
    mongo_uri = "mongodb://%s:%s@%s" % (quote_plus(mongo_user), quote_plus(mongo_pass_word), mongo_end_point)
    client = MongoClient(mongo_uri)
    db = client['Auth']
    collection = db['fernetKey']
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit(1)

document = {
    'key': key.decode(),
    'environment': "prod"
}

try:
    collection.insert_one(document)
    print("Fernet key inserted successfully.")
except Exception as e:
    print(f"Error inserting data: {e}")

client.close()
