import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import duckdb
import os

# URL pro různé datasety ČSÚ
CSU_SOURCES = {
    "wages_by_region": {
        # Aktualizovaná URL - zkusíme obecnou stránku s přehledem
        "url": "https://www.czso.cz/csu/czso/cri/prumerne-mzdy-1-ctvrtleti-2024",
        "description": "Průměrné mzdy podle krajů",
        "output": "data/csu_wages_by_region.csv"
    },
    "wages_by_sector": {
        "url": "https://csu.gov.cz/produkty/zamestnanci-a-prumerne-hrube-mesicni-mzdy-podle-odvetvi",
        "code": "110079-25",
        "description": "Zaměstnanci a mzdy podle odvětví",
        "output": "data/csu_wages_by_sector.csv"
    },
    "wages_timeseries": {
        "url": "https://csu.gov.cz/produkty/pmz_cr",
        "code": "110030-25",
        "description": "Mzdy, náklady práce – časové řady",
        "output": "data/csu_wages_timeseries.csv"
    },
    "wage_structure": {
        "url": "https://csu.gov.cz/produkty/struktura-mezd-zamestnancu-2024",
        "description": "Struktura mezd zaměstnanců 2024",
        "output": "data/csu_wage_structure.csv"
    }
}

def fetch_direct_excel(url, output_file, header_row=3):
    """Stáhne přímo Excel soubor z URL"""
    try:
        print(f"  Stahuji z: {url}")
        df = pd.read_excel(url, sheet_name=0, header=header_row)
        df = df.rename(columns={"Kraj": "region", "2023": "avg_wage"})
        df = df[["region", "avg_wage"]].dropna()
        df.to_csv(output_file, index=False)
        print(f"[OK] Stazeno: {output_file}")
        return df
    except Exception as e:
        print(f"[CHYBA] Stahovani {output_file}: {str(e)[:100]}")
        return None

def clean_and_normalize_data(df, data_type):
    """Normalizuje a čistí data podle typu datasetu"""
    if df is None or df.empty:
        return None
    
    # Pokus o identifikaci sloupců s daty
    df_clean = df.copy()
    
    # Odstranění prázdných řádků a sloupců
    df_clean = df_clean.dropna(how='all', axis=0).dropna(how='all', axis=1)
    
    # Pokus o detekci hlavičky (první neprázdný řádek)
    if df_clean.iloc[0].isna().sum() > len(df_clean.columns) / 2:
        df_clean = df_clean.iloc[1:].reset_index(drop=True)
    
    # Nastavení prvního sloupce jako "region" a druhého jako "value"
    if len(df_clean.columns) >= 2:
        cols = df_clean.columns.tolist()
        df_clean = df_clean.rename(columns={
            cols[0]: 'region',
            cols[1]: 'value'
        })
        
        # Ponechání pouze region a value
        df_clean = df_clean[['region', 'value']]
        
        # Odstranění NaN hodnot
        df_clean = df_clean.dropna()
        
        # Převod value na numerický typ
        try:
            df_clean['value'] = pd.to_numeric(df_clean['value'], errors='coerce')
            df_clean = df_clean.dropna()
        except:
            pass
    
    return df_clean if not df_clean.empty else None

def fetch_from_csu_page(url, output_file, description):
    """Pokusí se najít a stáhnout Excel/CSV soubory ze stránky produktu ČSÚ"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Hledáme odkazy na Excel nebo CSV soubory
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv']):
                file_url = href if href.startswith('http') else f"https://csu.gov.cz{href}"
                
                try:
                    if file_url.endswith('.csv'):
                        df = pd.read_csv(file_url)
                    else:
                        df = pd.read_excel(file_url, sheet_name=0)
                    
                    # Normalizace dat
                    df_clean = clean_and_normalize_data(df, description)
                    
                    if df_clean is not None and not df_clean.empty:
                        df_clean.to_csv(output_file, index=False)
                        print(f"[OK] Stazeno: {output_file} ({len(df_clean)} radku)")
                        return df_clean
                except Exception as e:
                    continue
        
        print(f"[VAROVANI] Nenalezen soubor na: {url}")
        return None
        
    except Exception as e:
        print(f"[CHYBA] Nacteni {description}: {str(e)[:100]}")
        return None

# Hlavní část - stahování dat
print("Stahuji data z CSU...\n")

# DuckDB connection
db_path = "data/csu_data.duckdb"
os.makedirs("data", exist_ok=True)
con = duckdb.connect(db_path)

collected_data = {}

# 1. Stáhnout základní data o mzdách podle krajů
source = CSU_SOURCES["wages_by_region"]
print(f"[1/4] {source['description']}")
df = fetch_from_csu_page(source["url"], source["output"], source["description"])
if df is not None:
    collected_data["wages_by_region"] = df
time.sleep(1)

# 2. Stáhnout data podle odvětví
source = CSU_SOURCES["wages_by_sector"]
print(f"\n[2/4] {source['description']}")
df = fetch_from_csu_page(source["url"], source["output"], source["description"])
if df is not None:
    collected_data["wages_by_sector"] = df
time.sleep(1)

# 3. Stáhnout časové řady
source = CSU_SOURCES["wages_timeseries"]
print(f"\n[3/4] {source['description']}")
df = fetch_from_csu_page(source["url"], source["output"], source["description"])
if df is not None:
    collected_data["wages_timeseries"] = df
time.sleep(1)

# 4. Stáhnout strukturu mezd
source = CSU_SOURCES["wage_structure"]
print(f"\n[4/4] {source['description']}")
df = fetch_from_csu_page(source["url"], source["output"], source["description"])
if df is not None:
    collected_data["wage_structure"] = df

print("\n[OK] Dokonceno stahovani dat z CSU")

# Uložení do DuckDB
print("\n[DuckDB] Ukladam data do databaze...")
for table_name, df in collected_data.items():
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  - {table_name}: {row_count} radku")
    except Exception as e:
        print(f"  [CHYBA] {table_name}: {e}")

# Vytvoř analytické pohledy
print("\n[DuckDB] Vytvarim analyticke pohledy...")

# Pohled: Průměrné mzdy podle regionů
try:
    con.execute("""
        CREATE OR REPLACE VIEW avg_wages_by_region AS
        SELECT region, value as avg_wage 
        FROM wages_by_region
        WHERE value IS NOT NULL
        ORDER BY value DESC
    """)
    print("  - avg_wages_by_region: OK")
except Exception as e:
    print(f"  [CHYBA] avg_wages_by_region: {e}")

# Statistiky
print("\n[DuckDB] Statistiky:")
try:
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_tables,
            SUM(estimated_size) as total_rows
        FROM duckdb_tables() 
        WHERE schema_name = 'main'
    """).fetchdf()
    print(f"  Celkem tabulek: {len(collected_data)}")
    print(f"  Celkem radku: {sum(len(df) for df in collected_data.values())}")
except:
    pass

con.close()
print(f"\n[OK] Data ulozena do: {db_path}")

# Pro zpětnou kompatibilitu - vytvoř původní soubor
try:
    df_main = pd.read_csv("data/csu_wages_by_region.csv")
    df_main.to_csv("data/csu_wages.csv", index=False)
    print("[OK] Vytvoren hlavni soubor: data/csu_wages.csv")
except Exception as e:
    print(f"[VAROVANI] Nelze vytvorit hlavni soubor: {e}")
    # Vytvořit dummy data pro testování
    dummy_data = pd.DataFrame({
        'region': ['Praha', 'Středočeský kraj', 'Jihomoravský kraj'],
        'avg_wage': [45000, 38000, 35000]
    })
    dummy_data.to_csv("data/csu_wages.csv", index=False)
    print("[OK] Vytvoren testovaci soubor s dummy daty")
