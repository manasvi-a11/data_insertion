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
    """Reads city data from a CSV file and inserts it into MongoDB."""
    procedure_df = read_csv_safe(file_path)
    
    if procedure_df is None or procedure_df.empty:
        print("No data found in the CSV file. Aborting insert.")
        return

    collection = db[collection_name]
    
    # Assuming that the CSV has a 'City' column for inserting data
    if 'Procedure' not in procedure_df.columns:
        print("The CSV file does not contain a 'Procedure' column. Aborting insert.")
        return

    for item in procedure_df['Procedure'].dropna():
        document = {
            'title': item,
            'status': 'verified',
            'createdBy': 'admin',
            'createdOn': get_current_indian_time()
        }

        try:
            collection.insert_one(document)
            print(f"Inserted document for city '{item}'")
        except Exception as e:
            print(f"Error inserting city '{item}': {e}")

def main():
    """Main function to initiate the MongoDB insertion from CSV."""
    mongo_client = get_mongo_client()
    
    try:
        db = mongo_client['NTConfiguration']
        collection_name = 'Procedure'
        csv_file_path = "Procedures & Consultation - Procedure.csv" 
        insert_documents_from_csv(csv_file_path, db, collection_name)
    finally:
        mongo_client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    main()
