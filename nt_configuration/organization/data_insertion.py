import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import pytz
from urllib.parse import quote_plus

def get_current_indian_time():
    """Returns the current time in Indian Standard Time (IST)."""
    return datetime.now(pytz.timezone('Asia/Kolkata'))

def get_mongo_client():
    """Establishes a connection to MongoDB and returns the client."""
    try:
        mongo_end_point = "ip-15-100-128-214.ap-south-1.compute.internal:27017"
        mongo_user = "nextenti_admin"
        mongo_pass_word = "NextEntiData_2023"
        mongo_uri = f"mongodb://{quote_plus(mongo_user)}:{quote_plus(mongo_pass_word)}@{mongo_end_point}"
        client = MongoClient(mongo_uri)
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        exit(1)

def read_csv_safe(file_path):
    """Safely reads a CSV file and returns a DataFrame."""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        print(f"File is empty: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return None

def insert_documents_from_csv(file_path, db, collection_name):
    """Reads organization data from a CSV file and inserts it into MongoDB."""
    organization_df = read_csv_safe(file_path)
    
    if organization_df is None or organization_df.empty:
        print("No data found in the CSV file. Aborting insert.")
        return

    collection = db[collection_name]
    
    for row in organization_df.itertuples(index=False):
        document = {
            'title': row[0], 
            'iconPath': row[1],    
            'rank': row[2],        
            'status': 'verified',
            'createdBy': 'admin',
            'createdOn': get_current_indian_time()
        }

        try:
            collection.insert_one(document)
            print(f"Inserted document for organization '{row[0]}'")
        except Exception as e:
            print(f"Error inserting organization '{row[0]}': {e}")

def main():
    """Main function to initiate the MongoDB insertion from CSV."""
    mongo_client = get_mongo_client()
    
    try:
        db = mongo_client['NTConfiguration']
        collection_name = 'Organization'
        csv_file_path = "NTConfiguration.Organization - NTConfiguration.Organization.csv.csv"  
        insert_documents_from_csv(csv_file_path, db, collection_name)
    finally:
        mongo_client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    main()
