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
    collection = db['PriorityWeightage']
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit(1)

csv_paths = {
    'Doctor': "Priority_Weightage - Doctor.csv",
    'Nurse': "Priority_Weightage - Nurse.csv",
    'Dentist': "Priority_Weightage - Dentist.csv",
    'Physiotherapy': "Priority_Weightage - Physiotherapy.csv",
    'Pharmacist': "Priority_Weightage - Pharmacist.csv",
    'Management / Administrator': "Priority_Weightage - Management _ Administrator.csv",
    'Dietitian': "Priority_Weightage - Dietitian.csv",
    'Paramedical / Technician': "Priority_Weightage - Paramedical _ Technicians.csv",
    'ICU / OT Staff': "Priority_Weightage - ICU _ OT Staff.csv",
    'Critical Care / Emergency Technicians & Staff': "Priority_Weightage - Critical Care _ Emergency Technicians & Staff.csv",
    'Human Resources': "Priority_Weightage - Human Resources.csv",
    'Accounts & Auditing': "Priority_Weightage - Accounts & Auditing.csv",
    'Billing & Insurance': "Priority_Weightage - Billing & Insurance.csv",
    'PR & Marketing': "Priority_Weightage - PR & Marketing.csv",
    'Facility Maintenance / Engineering': "Priority_Weightage - Facility Maintanence _ Engineering.csv",
    'Biomedical Staff': "Priority_Weightage - Biomedical Staff.csv",
    'IT & Medical Records': "Priority_Weightage - IT & Medical Records.csv",
    'Lab & Radiology': "Priority_Weightage - Lab & Radiology.csv",
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
        priority_dict = {}
        
        for index, row in df.iterrows():
            job_spec = row.get('Job Specifications')
            priority_weight = row.get('Priority Weightage')

            if pd.notna(job_spec) and pd.notna(priority_weight):
                priority_dict[job_spec] = priority_weight

        document = {
            'profession': profession,
            'priorityWeightage': priority_dict
        }

        try:
            collection.insert_one(document)
            print(f"Data for {profession} inserted successfully.")
        except Exception as e:
            print(f"Error inserting data for {profession}: {e}")

client.close()
