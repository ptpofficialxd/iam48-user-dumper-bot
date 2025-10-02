import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CSV_FILE = os.getenv("CSV_FILE")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_csv():
    df = pd.read_csv(CSV_FILE)
    df = df.where(pd.notnull(df), None)
    df = df.drop_duplicates(subset=["id"], keep="last")
    records = df.to_dict(orient="records")
    BATCH_SIZE = 1000
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            supabase.table("users").upsert(batch).execute()
            print(f"[✅] Upsert {len(batch)} rows ({i+1} - {i+len(batch)})")
        except Exception as e:
            print(f"[❌] Error batch {i+1} - {i+len(batch)}: {e}")

if __name__ == "__main__":
    upsert_csv()
    print("[📂] Import complete: CSV has been upserted to Supabase")
