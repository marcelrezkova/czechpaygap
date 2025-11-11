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

def upload_csv_to_supabase(csv_path, table_name, columns=None):
    """Upload CSV souboru do Supabase tabulky"""
    try:
        if not os.path.exists(csv_path):
            print(f"[SKIP] Soubor {csv_path} neexistuje, preskakuji...")
            return False
        
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"[SKIP] Soubor {csv_path} je prazdny, preskakuji...")
            return False
        
        # Pokud jsou specifikovány sloupce, vybereme jen ty
        if columns:
            available_cols = [col for col in columns if col in df.columns]
            df = df[available_cols]
        
        # Odstranění NaN hodnot (nahradíme None, což je JSON kompatibilní)
        df = df.replace({pd.NA: None, float('nan'): None})
        # Alternativně použijeme where
        df = df.where(pd.notna(df), None)
        
        print(f"[UPLOAD] {len(df)} rows -> {table_name}")
        supabase.table(table_name).upsert(df.to_dict(orient="records")).execute()
        print("[OK] Done")
        return True
    except Exception as e:
        print(f"[ERROR] Chyba pri nahravani {csv_path}: {e}")
        return False

if __name__ == "__main__":
    print("="*60)
    print("NAHRAVANI DAT DO SUPABASE")
    print("="*60 + "\n")
    
    # Upload ČSÚ dat
    print("--- CSU Data ---")
    # csu_wages_by_region a csu_wages jsou stejná data, jen jiný název tabulky
    # Nahrávat jen jednou do csu_wages
    # upload_csv_to_supabase("data/csu_wages_by_region.csv", TABLES["csu_wages_by_region"])
    upload_csv_to_supabase("data/csu_wages.csv", "csu_wages")
    
    upload_csv_to_supabase("data/csu_wages_by_sector.csv", TABLES["csu_wages_by_sector"])
    upload_csv_to_supabase("data/csu_wages_timeseries.csv", TABLES["csu_wages_timeseries"])
    # wage_structure má špatnou strukturu, přeskakujeme
    # upload_csv_to_supabase("data/csu_wage_structure.csv", TABLES["csu_wage_structure"])
    
    # Upload dat z pracovních portálů
    print("\n--- Job Listings Data ---")
    # job_listings tabulka očekává: region, salary_offer, source, job_title
    upload_csv_to_supabase("data/job_listings.csv", TABLES["job_listings"], 
                          columns=["region", "salary_offer", "source", "job_title"])
    
    print("\n[OK] Vsechna data nahrana do Supabase")
    print("="*60)
