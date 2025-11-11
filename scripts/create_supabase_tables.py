"""
Vytvoří tabulky v Supabase databázi
Používá anon key pro vytvoření základní struktury
"""

import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def create_tables():
    """Vytvoří všechny potřebné tabulky v Supabase"""
    
    print("🔧 Připojování k Supabase...")
    print(f"URL: {SUPABASE_URL}")
    
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Připojeno k Supabase\n")
    except Exception as e:
        print(f"❌ Chyba při připojení: {e}")
        return False
    
    # SQL příkazy pro vytvoření tabulek
    tables = {
        "csu_wages": """
            CREATE TABLE IF NOT EXISTS csu_wages (
                id BIGSERIAL PRIMARY KEY,
                region TEXT NOT NULL,
                value DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(region)
            );
        """,
        
        "csu_wages_by_sector": """
            CREATE TABLE IF NOT EXISTS csu_wages_by_sector (
                id BIGSERIAL PRIMARY KEY,
                region TEXT,
                value DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
        
        "csu_wages_timeseries": """
            CREATE TABLE IF NOT EXISTS csu_wages_timeseries (
                id BIGSERIAL PRIMARY KEY,
                region TEXT,
                value DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
        
        "csu_wage_structure": """
            CREATE TABLE IF NOT EXISTS csu_wage_structure (
                id BIGSERIAL PRIMARY KEY,
                region TEXT,
                value DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
        
        "job_listings": """
            CREATE TABLE IF NOT EXISTS job_listings (
                id BIGSERIAL PRIMARY KEY,
                region TEXT NOT NULL,
                salary_offer INTEGER NOT NULL,
                source TEXT,
                job_title TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
        
        "wages_comparison": """
            CREATE TABLE IF NOT EXISTS wages_comparison (
                id BIGSERIAL PRIMARY KEY,
                region TEXT NOT NULL,
                avg_offer DECIMAL(10, 2),
                median_offer DECIMAL(10, 2),
                min_offer INTEGER,
                max_offer INTEGER,
                pay_gap DECIMAL(10, 2),
                pay_gap_pct DECIMAL(5, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(region)
            );
        """,
        
        "wages_by_source": """
            CREATE TABLE IF NOT EXISTS wages_by_source (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                avg_salary DECIMAL(10, 2),
                min_salary INTEGER,
                max_salary INTEGER,
                job_count INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(source)
            );
        """
    }
    
    print("📋 Vytváření tabulek...\n")
    
    for table_name, sql in tables.items():
        try:
            print(f"  Creating {table_name}...", end=" ")
            
            # Supabase Python klient nepodporuje přímé SQL DDL
            # Musíme použít REST API nebo Supabase dashboard
            # Pro teď jen ověříme, že tabulka existuje pokusem o select
            
            result = supabase.table(table_name).select("*").limit(1).execute()
            print("✅ Již existuje nebo vytvořena")
            
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                print(f"⚠️  Neexistuje - musí být vytvořena v Supabase SQL Editoru")
                print(f"      SQL: {sql[:80]}...")
            else:
                print(f"❌ Chyba: {e}")
    
    print("\n" + "="*60)
    print("⚠️  DŮLEŽITÉ: Tabulky nelze vytvořit pomocí anon key!")
    print("="*60)
    print("\n📝 Postup:")
    print("1. Přejděte do Supabase Dashboard:")
    print(f"   {SUPABASE_URL.replace('https://', 'https://app.supabase.com/project/')}")
    print("\n2. Klikněte na 'SQL Editor' v levém menu")
    print("\n3. Spusťte následující SQL příkazy:")
    print("\n" + "-"*60)
    
    for table_name, sql in tables.items():
        print(f"\n-- {table_name}")
        print(sql.strip())
    
    print("\n" + "-"*60)
    print("\nNebo použijte soubor: setup_tables.sql")
    print("\n✅ Po vytvoření tabulek můžete spustit upload pomocí:")
    print("   python pipeline/step1_upload.py")

if __name__ == "__main__":
    create_tables()
