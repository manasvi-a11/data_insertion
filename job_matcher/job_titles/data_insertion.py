import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus

try:
    mongo_end_point = "ip-15-100-128-214.ap-south-1.compute.internal:27017"
    mongo_user = "nextenti_admin"
    mongo_pass_word = "NextEntiData_2023"
    mongo_uri = "mongodb://%s:%s@%s" % (quote_plus(mongo_user), quote_plus(mongo_pass_word), mongo_end_point)
    client = MongoClient(mongo_uri)
    db = client['JobMatcher']
    collection = db['JobTitleSynonyms']
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit(1)

csv_paths = {
    'Doctor': "Re-Structured Data for Job Matching  - Doctor.csv",
    'Nurse': "Re-Structured Data for Job Matching  - Nurse.csv",
    'Dentist': "Re-Structured Data for Job Matching  - Dentist.csv",
    'Physiotherapy': "Re-Structured Data for Job Matching  - Physiotherapy.csv",
    'Pharmacist': "Re-Structured Data for Job Matching  - Pharmacist.csv",
    'Management / Administrator': "Re-Structured Data for Job Matching  - Management _ Administrator.csv",
    'Dietitian': "Re-Structured Data for Job Matching  - Dietitian.csv",
    'Paramedical / Technician': "Re-Structured Data for Job Matching  - Paramedical _ Technician.csv",
    'ICU / OT Staff': "Re-Structured Data for Job Matching  - ICU _ OT Staff.csv",
    'Critical Care / Emergency Technicians & Staff': "Re-Structured Data for Job Matching  - Critical Care _ Emergency Technicians & Staff.csv",
    'Human Resources': "Re-Structured Data for Job Matching  - Human Resources.csv",
    'Accounts & Auditing': "Re-Structured Data for Job Matching  - Accounts & Auditing.csv",
    'Billing & Insurance': "Re-Structured Data for Job Matching  - Billing & Insurance.csv",
    'PR & Marketing': "Re-Structured Data for Job Matching  - PR & Marketing.csv",
    'Facility Maintenance / Engineering': "Re-Structured Data for Job Matching  - Facility Maintanence _ Engineering.csv",
    'Biomedical Staff': "Re-Structured Data for Job Matching  - Biomedical Staff.csv",
    'IT & Medical Records': "Re-Structured Data for Job Matching  - IT & Medical Records.csv",
    'Lab & Radiology': "Re-Structured Data for Job Matching  - Lab & Radiology.csv",
}

def read_csv_safe(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

profession_dict = {prof: read_csv_safe(path) for prof, path in csv_paths.items()}

for profession, df in profession_dict.items():
    if df is not None:
        synonyms_dict = {}
        
        for column in df.columns:
            synonyms_dict[column.lower()] = [str(item).lower() for item in df[column].dropna()]

        document = {
            'profession': profession,
            'synonymsData': synonyms_dict
        }

        try:
            collection.insert_one(document)
            print(f"Data for {profession} inserted successfully.")
        except Exception as e:
            print(f"Error inserting data for {profession}: {e}")

client.close()
