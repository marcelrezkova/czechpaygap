# pipeline/step1_upload.py
import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Definice tabulek pro různé datasety
TABLES = {
    "csu_wages_by_region": "csu_wages",
    "csu_wages_by_sector": "csu_wages_by_sector",
    "csu_wages_timeseries": "csu_wages_timeseries",
    "csu_wage_structure": "csu_wage_structure",
    "job_listings": "job_listings"
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_csv_to_supabase(csv_path, table_name):
    """Upload CSV souboru do Supabase tabulky"""
    try:
        if not os.path.exists(csv_path):
            print(f"⚠️  Soubor {csv_path} neexistuje, přeskakuji...")
            return False
        
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"⚠️  Soubor {csv_path} je prázdný, přeskakuji...")
            return False
            
        print(f"🧩 Uploading {len(df)} rows to Supabase → {table_name}")
        supabase.table(table_name).upsert(df.to_dict(orient="records")).execute()
        print("✅ Done")
        return True
    except Exception as e:
        print(f"✗ Chyba při nahrávání {csv_path}: {e}")
        return False

if __name__ == "__main__":
    print("📤 Nahrávání dat do Supabase...\n")
    
    # Upload ČSÚ dat
    print("--- ČSÚ Data ---")
    upload_csv_to_supabase("data/csu_wages_by_region.csv", TABLES["csu_wages_by_region"])
    upload_csv_to_supabase("data/csu_wages_by_sector.csv", TABLES["csu_wages_by_sector"])
    upload_csv_to_supabase("data/csu_wages_timeseries.csv", TABLES["csu_wages_timeseries"])
    upload_csv_to_supabase("data/csu_wage_structure.csv", TABLES["csu_wage_structure"])
    
    # Zpětná kompatibilita - hlavní soubor csu_wages
    upload_csv_to_supabase("data/csu_wages.csv", "csu_wages")
    
    # Upload dat z pracovních portálů
    print("\n--- Job Listings Data ---")
    upload_csv_to_supabase("data/job_listings.csv", TABLES["job_listings"])
    
    print("\n✅ Všechna data nahrána do Supabase")
