"""
Skript pro vytvoření všech potřebných tabulek v Supabase
Spusťte tento skript před prvním spuštěním pipeline
"""
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# SQL příkazy pro vytvoření tabulek
CREATE_TABLES_SQL = """
-- Tabulka: csu_wages (průměrné mzdy podle krajů)
CREATE TABLE IF NOT EXISTS csu_wages (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    avg_wage DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(region)
);

-- Tabulka: csu_wages_by_sector (mzdy podle odvětví)
CREATE TABLE IF NOT EXISTS csu_wages_by_sector (
    id BIGSERIAL PRIMARY KEY,
    region TEXT,
    sector TEXT,
    avg_wage DECIMAL(10, 2),
    employees INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: csu_wages_timeseries (časové řady)
CREATE TABLE IF NOT EXISTS csu_wages_timeseries (
    id BIGSERIAL PRIMARY KEY,
    region TEXT,
    year INTEGER,
    quarter INTEGER,
    avg_wage DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: csu_wage_structure (struktura mezd)
CREATE TABLE IF NOT EXISTS csu_wage_structure (
    id BIGSERIAL PRIMARY KEY,
    region TEXT,
    category TEXT,
    value DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: job_listings (pracovní nabídky)
CREATE TABLE IF NOT EXISTS job_listings (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    salary_offer INTEGER NOT NULL,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: wages_by_source (agregace podle zdroje)
CREATE TABLE IF NOT EXISTS wages_by_source (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    source TEXT NOT NULL,
    avg_offer DECIMAL(10, 2),
    offers INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(region, source)
);

-- Tabulka: wages_comparison (výsledné metriky)
CREATE TABLE IF NOT EXISTS wages_comparison (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL UNIQUE,
    avg_wage DECIMAL(10, 2),
    avg_offer DECIMAL(10, 2),
    median_offer DECIMAL(10, 2),
    min_offer INTEGER,
    max_offer INTEGER,
    offers INTEGER,
    pay_gap DECIMAL(10, 2),
    pay_gap_pct DECIMAL(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexy pro rychlejší dotazy
CREATE INDEX IF NOT EXISTS idx_job_listings_region ON job_listings(region);
CREATE INDEX IF NOT EXISTS idx_job_listings_source ON job_listings(source);
CREATE INDEX IF NOT EXISTS idx_wages_comparison_region ON wages_comparison(region);
CREATE INDEX IF NOT EXISTS idx_wages_by_source_region_source ON wages_by_source(region, source);
"""

def create_tables():
    """Vytvoří všechny tabulky v Supabase pomocí SQL"""
    print("🏗️  Vytvářím tabulky v Supabase...\n")
    
    try:
        # Poznámka: Supabase Python klient nepodporuje přímé SQL DDL příkazy
        # Musíme použít Supabase SQL Editor nebo API
        print("⚠️  DŮLEŽITÉ:")
        print("   Supabase Python klient neumožňuje spouštět CREATE TABLE příkazy.")
        print("   Musíte vytvořit tabulky manuálně nebo použít SQL Editor.\n")
        
        print("📋 Zkopírujte následující SQL kód do Supabase SQL Editor:")
        print("   https://supabase.com/dashboard/project/YOUR_PROJECT/sql/new\n")
        print("-" * 70)
        print(CREATE_TABLES_SQL)
        print("-" * 70)
        
        # Alternativně vytvoříme SQL soubor
        with open("setup_tables.sql", "w", encoding="utf-8") as f:
            f.write(CREATE_TABLES_SQL)
        
        print("\n✅ SQL kód byl také uložen do souboru: setup_tables.sql")
        print("\n📝 Kroky:")
        print("   1. Otevřete Supabase Dashboard → SQL Editor")
        print("   2. Zkopírujte obsah setup_tables.sql")
        print("   3. Spusťte SQL (Run)")
        print("   4. Poté můžete spustit: python pipeline/run_pipeline.py")
        
    except Exception as e:
        print(f"❌ Chyba: {e}")

if __name__ == "__main__":
    create_tables()
