"""
Vylepšený scraper s paralelním stahováním a DuckDB
Scrapuje všechny dostupné pracovní pozice z českých portálů
"""
import asyncio
import aiohttp
import requests
import re
import time
from bs4 import BeautifulSoup
import pandas as pd
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed

regions = {
    "praha": "Praha",
    "stredocesky": "Středočeský kraj",
    "jihocesky": "Jihočeský kraj",
    "plzensky": "Plzeňský kraj",
    "karlovarsky": "Karlovarský kraj",
    "ustecky": "Ústecký kraj",
    "liberecky": "Liberecký kraj",
    "kralovehradecky": "Královéhradecký kraj",
    "pardubicky": "Pardubický kraj",
    "vysocina": "Kraj Vysočina",
    "jihomoravsky": "Jihomoravský kraj",
    "olomoucky": "Olomoucký kraj",
    "moravskoslezsky": "Moravskoslezský kraj",
    "zlinsky": "Zlínský kraj",
}

def extract_salary_from_text(text):
    """Extrahuje plat z textu"""
    m = re.search(r"(\d[\d\s]+)", str(text))
    if m:
        salary_str = m.group(1).replace(" ", "").replace("\xa0", "")
        try:
            salary = int(salary_str)
            if 15000 <= salary <= 200000:
                return salary
        except ValueError:
            pass
    return None

def scrape_prace_cz(slug, region, max_pages=10):
    """Scrape jeden region z prace.cz - všechny stránky včetně job titles"""
    results = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    for page in range(1, max_pages + 1):
        try:
            url = f"https://www.prace.cz/nabidky/?region={slug}&page={page}"
            r = requests.get(url, timeout=10, headers=headers)
            soup = BeautifulSoup(r.text, "html.parser")

            page_results = 0
            
            # Vytvoříme mapu job title -> link pro pozdější spojení
            job_titles_map = {}
            job_links = soup.find_all('a', href=re.compile(r'/nabidka/'))
            for link in job_links:
                title = link.get_text(strip=True)
                if title:
                    job_titles_map[id(link.find_parent())] = title
            
            # Scrape platy (původní metoda)
            for s in soup.find_all(string=re.compile(r"\d{2,}\s*(?:Kč|CZK)", re.I)):
                salary = extract_salary_from_text(s)
                if salary:
                    # Zkus najít job title
                    job_title = None
                    parent = s.find_parent()
                    if parent and id(parent) in job_titles_map:
                        job_title = job_titles_map[id(parent)]
                    
                    results.append({
                        "region": region,
                        "salary_offer": salary,
                        "job_title": job_title[:200] if job_title else None,
                        "source": "prace.cz",
                        "page": page
                    })
                    page_results += 1
            
            # Pokud na stránce není žádná nabídka, končíme
            if page_results == 0:
                break
                
            time.sleep(0.5)  # Menší pauza mezi stránkami
            
        except Exception as e:
            print(f"  [!] prace.cz/{slug}/page{page}: {e}")
            break
    
    return results

def scrape_jobs_cz(slug, region, max_pages=10):
    """Scrape jeden region z jobs.cz - všechny stránky včetně job titles"""
    results = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    for page in range(1, max_pages + 1):
        try:
            # Jobs.cz používá jiný pagination parametr
            url = f"https://www.jobs.cz/prace/?locality%5B%5D={slug}&page={page}"
            r = requests.get(url, timeout=10, headers=headers)
            soup = BeautifulSoup(r.text, "html.parser")
            
            page_results = 0
            
            # Najdeme všechny pracovní nabídky
            job_cards = soup.find_all(['article', 'div'], class_=re.compile(r'offer|job|listing|search', re.I))
            
            if not job_cards:
                # Fallback
                for s in soup.find_all(string=re.compile(r"\d{2,}\s*(?:Kč|CZK)", re.I)):
                    salary = extract_salary_from_text(s)
                    if salary:
                        results.append({
                            "region": region,
                            "salary_offer": salary,
                            "job_title": None,
                            "source": "jobs.cz",
                            "page": page
                        })
                        page_results += 1
            else:
                for card in job_cards:
                    # Hledej titulek
                    title_elem = card.find(['h2', 'h3', 'a'], class_=re.compile(r'title|name|position', re.I))
                    job_title = title_elem.get_text(strip=True) if title_elem else None
                    
                    # Hledej plat
                    salary = None
                    for s in card.find_all(string=re.compile(r"\d{2,}\s*(?:Kč|CZK)", re.I)):
                        salary = extract_salary_from_text(s)
                        if salary:
                            break
                    
                    if salary:
                        results.append({
                            "region": region,
                            "salary_offer": salary,
                            "job_title": job_title[:200] if job_title else None,
                            "source": "jobs.cz",
                            "page": page
                        })
                        page_results += 1
            
            # Pokud na stránce není žádná nabídka, končíme
            if page_results == 0:
                break
                
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  [!] jobs.cz/{slug}/page{page}: {e}")
            break
    
    return results

def scrape_generic_portal(domain, max_pages=5):
    """Scrape obecný portál - více stránek včetně job titles"""
    results = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    for page in range(1, max_pages + 1):
        try:
            # Různé portály mají různé pagination struktury
            if page == 1:
                url = f"https://www.{domain}/prace"
            else:
                url = f"https://www.{domain}/prace?page={page}"
                
            r = requests.get(url, timeout=10, headers=headers)
            soup = BeautifulSoup(r.text, "html.parser")
            
            page_results = 0
            
            # Pokus o strukturovaný scraping
            job_cards = soup.find_all(['article', 'div'], class_=re.compile(r'offer|job|listing', re.I))
            
            if not job_cards:
                # Fallback
                for text in soup.find_all(string=re.compile(r"\d{2,}\s*(?:Kč|CZK)", re.I)):
                    salary = extract_salary_from_text(text)
                    if salary:
                        results.append({
                            "region": "Neznamy",
                            "salary_offer": salary,
                            "job_title": None,
                            "source": domain,
                            "page": page
                        })
                        page_results += 1
            else:
                for card in job_cards:
                    # Hledej titulek
                    title_elem = card.find(['h2', 'h3', 'a'], class_=re.compile(r'title|name|position', re.I))
                    job_title = title_elem.get_text(strip=True) if title_elem else None
                    
                    # Hledej plat
                    salary = None
                    for text in card.find_all(string=re.compile(r"\d{2,}\s*(?:Kč|CZK)", re.I)):
                        salary = extract_salary_from_text(text)
                        if salary:
                            break
                    
                    if salary:
                        results.append({
                            "region": "Neznamy",
                            "salary_offer": salary,
                            "job_title": job_title[:200] if job_title else None,
                            "source": domain,
                            "page": page
                        })
                        page_results += 1
            
            if page_results == 0:
                break
                
            time.sleep(1)
            
        except Exception as e:
            if page == 1:  # Logovat jen chybu na první stránce
                print(f"  [!] {domain}: {e}")
            break
    
    return results

def parallel_scrape():
    """Paralelní scraping všech zdrojů"""
    all_results = []
    
    print("[1/3] Scrapuji prace.cz (paralelne - az 10 stranek na kraj)...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_prace_cz, slug, region, 10): region 
                   for slug, region in regions.items()}
        
        completed = 0
        total = len(futures)
        for future in as_completed(futures):
            region = futures[future]
            completed += 1
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
                    print(f"  [{completed}/{total}] {region}: {len(results)} nabidek")
            except Exception as e:
                print(f"  [{completed}/{total}] [!] {region}: {e}")
    
    print(f"\n[2/3] Scrapuji jobs.cz (paralelne - az 10 stranek na kraj)...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_jobs_cz, slug, region, 10): region 
                   for slug, region in regions.items()}
        
        completed = 0
        total = len(futures)
        for future in as_completed(futures):
            region = futures[future]
            completed += 1
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
                    print(f"  [{completed}/{total}] {region}: {len(results)} nabidek")
            except Exception as e:
                print(f"  [{completed}/{total}] [!] {region}: {e}")
    
    print(f"\n[3/3] Scrapuji ostatni portaly (az 5 stranek)...")
    other_portals = ["profesia.cz", "startupjobs.cz", "dobraprace.cz"]
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(scrape_generic_portal, domain, 5): domain 
                   for domain in other_portals}
        
        completed = 0
        total = len(futures)
        for future in as_completed(futures):
            domain = futures[future]
            completed += 1
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
                    print(f"  [{completed}/{total}] {domain}: {len(results)} nabidek")
            except Exception as e:
                print(f"  [{completed}/{total}] [!] {domain}: {e}")
    
    return all_results

def save_to_duckdb(data):
    """Uloží data do DuckDB pro rychlé zpracování"""
    if not data:
        print("\n[VAROVANI] Zadna data k ulozeni")
        return None
    
    print(f"\n[DuckDB] Zpracovavam {len(data)} zaznamu...")
    
    # Vytvoř DataFrame
    df = pd.DataFrame(data)
    
    # DuckDB zpracování
    con = duckdb.connect('data/jobs.duckdb')
    
    # Vytvoř tabulku (drop a znovu vytvoř pro změnu schématu)
    con.execute("DROP TABLE IF EXISTS job_listings")
    con.execute("""
        CREATE TABLE job_listings (
            region VARCHAR,
            salary_offer INTEGER,
            job_title VARCHAR,
            source VARCHAR,
            page INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Vložit data
    con.execute("DELETE FROM job_listings")  # Clear old data
    con.execute("INSERT INTO job_listings (region, salary_offer, job_title, source, page) SELECT region, salary_offer, job_title, source, page FROM df")
    
    # Statistiky
    stats = con.execute("""
        SELECT 
            source,
            COUNT(*) as count,
            AVG(salary_offer) as avg_salary,
            MIN(salary_offer) as min_salary,
            MAX(salary_offer) as max_salary
        FROM job_listings
        GROUP BY source
        ORDER BY count DESC
    """).fetchdf()
    
    print("\n[Statistiky podle zdroje]")
    print(stats.to_string(index=False))
    
    # Export do CSV
    con.execute("COPY job_listings TO 'data/job_listings.csv' (HEADER, DELIMITER ',')") 
    
    con.close()
    print(f"\n[OK] Data ulozena do:")
    print("  - data/jobs.duckdb")
    print("  - data/job_listings.csv")
    
    return df

if __name__ == "__main__":
    print("="*60)
    print("CzechPayGap - Vylepšený scraper s DuckDB")
    print("="*60)
    
    start_time = time.time()
    
    # Paralelní scraping
    results = parallel_scrape()
    
    # Uložit do DuckDB a CSV
    df = save_to_duckdb(results)
    
    elapsed = time.time() - start_time
    
    if df is not None:
        print(f"\n{'='*60}")
        print(f"[DOKONCENO] {len(df)} nabidek z {df['source'].nunique()} zdroju")
        print(f"[CAS] {elapsed:.1f} sekund")
        print(f"{'='*60}")
    else:
        print("\n[CHYBA] Scraping selhal, vytvarim testovaci data...")
        test_data = []
        for region in list(regions.values()):
            for _ in range(20):
                test_data.append({
                    "region": region,
                    "salary_offer": 30000 + (hash(region) % 30000),
                    "source": "test_data"
                })
        df = pd.DataFrame(test_data)
        df.to_csv("data/job_listings.csv", index=False)
        print(f"[OK] Vytvoren testovaci dataset: {len(df)} radku")
