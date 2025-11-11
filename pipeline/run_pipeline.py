# pipeline/run_pipeline.py
import subprocess
import datetime
import sys
import os

LOG_FILE = "pipeline/pipeline_log.txt"

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def run_script(script_path, description):
    """Spustí Python script a loguje výsledek"""
    log(f"▶️  {description}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    
    # Zobraz output
    if result.stdout:
        print(result.stdout)
    
    if result.returncode == 0:
        log(f"✅ {description} - OK")
        return True
    else:
        log(f"❌ Error in {description}")
        if result.stderr:
            log(f"   Error: {result.stderr}")
            print(result.stderr)
        return False

def ensure_data_folder():
    """Vytvoří složku data/ pokud neexistuje"""
    os.makedirs("data", exist_ok=True)

if __name__ == "__main__":
    log("=" * 60)
    log("🚀 CzechPayGap Pipeline Start")
    log("=" * 60)
    
    # Zajisti existenci složky data/
    ensure_data_folder()
    
    # KROK 1: Stáhnout data z ČSÚ
    if not run_script("scripts/fetch_csu_data.py", "Stahování dat z ČSÚ"):
        log("⚠️  Pipeline pokračuje i přes chybu v ČSÚ datech...")
    
    # KROK 2: Scrape pracovní nabídky
    if not run_script("scripts/scrape_job_offers.py", "Scraping pracovních nabídek"):
        log("⚠️  Pipeline pokračuje i přes chybu ve scrapingu...")
    
    # KROK 3: Upload dat do Supabase
    if not run_script("pipeline/step1_upload.py", "Upload dat do Supabase"):
        log("❌ Selhání uploadu - ukončuji pipeline")
        sys.exit(1)
    
    # KROK 4: Výpočet metrik
    if not run_script("pipeline/step2_metrics.py", "Výpočet metrik a pay gap"):
        log("❌ Selhání výpočtu metrik - ukončuji pipeline")
        sys.exit(1)
    
    log("=" * 60)
    log("🎯 Pipeline finished successfully!")
    log("=" * 60)
