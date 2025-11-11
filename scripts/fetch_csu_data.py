import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# URL pro různé datasety ČSÚ
CSU_SOURCES = {
    "wages_by_region": {
        "url": "https://www.czso.cz/documents/10180/23159763/1100262301.xlsx",
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
        df = pd.read_excel(url, sheet_name=0, header=header_row)
        df = df.rename(columns={"Kraj": "region", "2023": "avg_wage"})
        df = df[["region", "avg_wage"]].dropna()
        df.to_csv(output_file, index=False)
        print(f"✓ Staženo: {output_file}")
        return True
    except Exception as e:
        print(f"✗ Chyba při stahování {output_file}: {e}")
        return False

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
                    
                    df.to_csv(output_file, index=False)
                    print(f"✓ Staženo: {output_file} z {file_url}")
                    return True
                except:
                    continue
        
        print(f"⚠ Nenalezen soubor ke stažení na: {url}")
        return False
        
    except Exception as e:
        print(f"✗ Chyba při načítání {description}: {e}")
        return False

# Hlavní část - stahování dat
print("Stahuji data z ČSÚ...\n")

# 1. Stáhnout základní data o mzdách podle krajů (původní)
source = CSU_SOURCES["wages_by_region"]
fetch_direct_excel(source["url"], source["output"])
time.sleep(1)

# 2. Stáhnout data podle odvětví
source = CSU_SOURCES["wages_by_sector"]
fetch_from_csu_page(source["url"], source["output"], source["description"])
time.sleep(1)

# 3. Stáhnout časové řady
source = CSU_SOURCES["wages_timeseries"]
fetch_from_csu_page(source["url"], source["output"], source["description"])
time.sleep(1)

# 4. Stáhnout strukturu mezd
source = CSU_SOURCES["wage_structure"]
fetch_from_csu_page(source["url"], source["output"], source["description"])

print("\n✓ Dokončeno stahování dat z ČSÚ")

# Pro zpětnou kompatibilitu - vytvoř původní soubor
try:
    df_main = pd.read_csv("data/csu_wages_by_region.csv")
    df_main.to_csv("data/csu_wages.csv", index=False)
    print("✓ Vytvořen hlavní soubor: data/csu_wages.csv")
except:
    pass
