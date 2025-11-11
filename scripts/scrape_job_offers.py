import requests, re, time
from bs4 import BeautifulSoup
import pandas as pd

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

# Mapování domén pro různé portály
domains = [
    "jobs.cz",
    "prace.cz",
    "profesia.cz",
    "volnamista.cz",
    "dobraprace.cz",
    "jooble.org",
    "indeed.com",
    "praceni.cz",
    "startupjobs.cz",
    "pracezarohem.cz",
    "jobdnes.cz",
    "flek.cz",
    "atmoskop.cz",
    "jenprace.cz",
    "avizo.cz",
    "inwork.cz",
    "nofluffjobs.com",
    "remote.co",
    "freelance.cz",
]

rows = []

# Scrape prace.cz
print("Scrapuji prace.cz...")
for slug, region in regions.items():
    try:
        url = f"https://www.prace.cz/nabidky/?region={slug}"
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for s in soup.select("div.offer__salary"):
            m = re.search(r"(\d[\d\s]*)", s.text)
            if m:
                salary = int(m.group(1).replace(" ", ""))
                rows.append({
                    "region": region,
                    "salary_offer": salary,
                    "source": "prace.cz"
                })
        time.sleep(2)
    except Exception as e:
        print(f"Chyba při scrape prace.cz pro {region}: {e}")

# Scrape jobs.cz
print("Scrapuji jobs.cz...")
for slug, region in regions.items():
    try:
        url = f"https://www.jobs.cz/prace/?locality%5B%5D={slug}"
        r = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.text, "html.parser")
        
        for s in soup.find_all(text=re.compile(r"\d+\s*(?:Kč|CZK)", re.I)):
            m = re.search(r"(\d[\d\s]*)", s)
            if m:
                salary = int(m.group(1).replace(" ", ""))
                if salary > 10000:  #Filter out unrealistic values
                    rows.append({
                        "region": region,
                        "salary_offer": salary,
                        "source": "jobs.cz"
                    })
        time.sleep(2)
    except Exception as e:
        print(f"Chyba při scrape jobs.cz pro {region}: {e}")

# Pro ostatní domény - obecné scrapování platu
print("Scrapuji ostatní domény...")
for domain in ["profesia.cz", "startupjobs.cz", "dobraprace.cz"]:
    try:
        url = f"https://www.{domain}"
        r = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Hledáme všechny texty obsahující částky
        for text in soup.find_all(text=re.compile(r"\d{2,}\s*(?:Kč|CZK|000)", re.I)):
            m = re.search(r"(\d[\d\s]*)", text)
            if m:
                salary = int(m.group(1).replace(" ", ""))
                if 15000 <= salary <= 200000:  # Realistický rozsah platů
                    rows.append({
                        "region": "Neznámý",
                        "salary_offer": salary,
                        "source": domain
                    })
        time.sleep(3)
    except Exception as e:
        print(f"Chyba při scrape {domain}: {e}")

df = pd.DataFrame(rows)
df.to_csv("data/job_listings.csv", index=False)
print(f"Staženo {len(df)} nabídek s platem z {df['source'].nunique()} zdrojů")