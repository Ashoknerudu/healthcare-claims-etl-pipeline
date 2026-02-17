import pandas as pd
import mysql.connector

# STEP 1: Read CSV
df = pd.read_csv("claims.csv")
print("CSV Loaded Successfully")

# STEP 2: Clean Data
df = df[df["claim_amount"] > 0]
print("Invalid claims removed")

# STEP 3: Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="healthuser",
    password="health123",
    database="healthcare_db"
)
cursor = conn.cursor()

# STEP 4: Insert Data
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO claims
        (claim_id, patient_id, provider_id, claim_amount,
         approved_amount, claim_status, service_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE claim_id = claim_id;
    """, (
        row["claim_id"],
        row["patient_id"],
        row["provider_id"],
        row["claim_amount"],
        row["approved_amount"],
        row["claim_status"],
        row["service_date"]
    ))

conn.commit()
conn.close()

print("Data inserted successfully")

