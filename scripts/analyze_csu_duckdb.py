"""
Analytické dotazy na ČSÚ data pomocí DuckDB
Rychlé SQL dotazy nad staženými daty z Českého statistického úřadu
"""

import duckdb
import sys

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def run_analysis():
    db_path = "data/csu_data.duckdb"
    
    try:
        con = duckdb.connect(db_path, read_only=True)
    except Exception as e:
        print(f"[CHYBA] Nelze otevrit databazi: {e}")
        print("Spustte nejdrive: python scripts/fetch_csu_data.py")
        sys.exit(1)
    
    # 1. Přehled tabulek
    print_section("Dostupné tabulky v databázi")
    tables = con.execute("""
        SELECT table_name, estimated_size 
        FROM duckdb_tables() 
        WHERE schema_name = 'main'
        ORDER BY table_name
    """).fetchdf()
    print(tables.to_string(index=False))
    
    # 2. Analýza mezd podle regionů
    print_section("Top 10 krajů podle průměrné mzdy")
    try:
        result = con.execute("""
            SELECT 
                region,
                value as avg_wage,
                value - (SELECT AVG(value) FROM wages_by_region) as diff_from_avg
            FROM wages_by_region
            WHERE value IS NOT NULL
            ORDER BY value DESC
            LIMIT 10
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"[VAROVANI] Tabulka wages_by_region neexistuje: {e}")
    
    # 3. Statistiky časových řad
    print_section("Mzdový vývoj v čase")
    try:
        result = con.execute("""
            SELECT 
                COUNT(*) as pocet_zaznamu,
                MIN(value) as min_mzda,
                MAX(value) as max_mzda,
                AVG(value) as prumer
            FROM wages_timeseries
            WHERE value IS NOT NULL
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"[VAROVANI] Tabulka wages_timeseries neexistuje: {e}")
    
    # 4. Struktura mezd
    print_section("Struktura mezd - základní statistiky")
    try:
        result = con.execute("""
            SELECT 
                COUNT(*) as pocet_zaznamu,
                COUNT(DISTINCT region) as pocet_regionu
            FROM wage_structure
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"[VAROVANI] Tabulka wage_structure neexistuje: {e}")
    
    # 5. Srovnání regionů - percentily
    print_section("Regionální rozdíly - percentilová analýza")
    try:
        result = con.execute("""
            SELECT 
                APPROX_QUANTILE(value, 0.25) as percentil_25,
                APPROX_QUANTILE(value, 0.50) as median,
                APPROX_QUANTILE(value, 0.75) as percentil_75,
                APPROX_QUANTILE(value, 0.90) as percentil_90
            FROM wages_by_region
            WHERE value IS NOT NULL
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"[VAROVANI] Nelze spocitat percentily: {e}")
    
    # 6. Export pro vizualizaci
    print_section("Export agregovaných dat")
    try:
        # Export top regionů do CSV pro streamlit
        con.execute("""
            COPY (
                SELECT 
                    region,
                    avg_wage,
                    RANK() OVER (ORDER BY avg_wage DESC) as rank
                FROM wages_by_region
                WHERE avg_wage IS NOT NULL
                ORDER BY avg_wage DESC
            ) TO 'data/csu_regional_summary.csv' (HEADER, DELIMITER ',')
        """)
        print("[OK] Exportovano: data/csu_regional_summary.csv")
    except Exception as e:
        print(f"[VAROVANI] Export selhal: {e}")
    
    con.close()
    print("\n" + "="*60)
    print("Analyza dokoncena!")
    print("="*60)

if __name__ == "__main__":
    run_analysis()
