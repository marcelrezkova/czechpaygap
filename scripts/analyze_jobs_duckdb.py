"""
DuckDB helper pro rychlou analýzu dat z job scrapingu
"""
import duckdb
import pandas as pd

def analyze_jobs():
    """Spustí různé analýzy nad job_listings daty"""
    
    con = duckdb.connect('data/jobs.duckdb', read_only=True)
    
    print("="*70)
    print("ANALÝZA DAT - Job Listings")
    print("="*70)
    
    # 1. Celkový přehled
    print("\n[1] Celkový přehled:")
    total = con.execute("SELECT COUNT(*) as total FROM job_listings").fetchone()[0]
    print(f"  Celkem nabídek: {total:,}")
    
    # 2. Podle regionu
    print("\n[2] Top 10 regionů podle počtu nabídek:")
    df_regions = con.execute("""
        SELECT 
            region,
            COUNT(*) as count,
            CAST(AVG(salary_offer) AS INTEGER) as avg_salary,
            CAST(MIN(salary_offer) AS INTEGER) as min_salary,
            CAST(MAX(salary_offer) AS INTEGER) as max_salary
        FROM job_listings
        WHERE region != 'Neznamy'
        GROUP BY region
        ORDER BY count DESC
        LIMIT 10
    """).fetchdf()
    print(df_regions.to_string(index=False))
    
    # 3. Podle zdroje
    print("\n[3] Nabídky podle zdroje:")
    df_sources = con.execute("""
        SELECT 
            source,
            COUNT(*) as count,
            CAST(AVG(salary_offer) AS INTEGER) as avg_salary,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM job_listings), 1) as percentage
        FROM job_listings
        GROUP BY source
        ORDER BY count DESC
    """).fetchdf()
    print(df_sources.to_string(index=False))
    
    # 4. Platové rozpětí
    print("\n[4] Platové statistiky:")
    stats = con.execute("""
        SELECT 
            CAST(AVG(salary_offer) AS INTEGER) as avg,
            CAST(MEDIAN(salary_offer) AS INTEGER) as median,
            CAST(MIN(salary_offer) AS INTEGER) as min,
            CAST(MAX(salary_offer) AS INTEGER) as max,
            CAST(STDDEV(salary_offer) AS INTEGER) as stddev
        FROM job_listings
    """).fetchdf()
    print(stats.to_string(index=False))
    
    # 5. Top 5 a Bottom 5 krajů podle průměrného platu
    print("\n[5] TOP 5 krajů s nejvyšším průměrným platem:")
    top5 = con.execute("""
        SELECT 
            region,
            CAST(AVG(salary_offer) AS INTEGER) as avg_salary,
            COUNT(*) as offers
        FROM job_listings
        WHERE region != 'Neznamy'
        GROUP BY region
        ORDER BY avg_salary DESC
        LIMIT 5
    """).fetchdf()
    print(top5.to_string(index=False))
    
    print("\n[6] BOTTOM 5 krajů s nejnižším průměrným platem:")
    bottom5 = con.execute("""
        SELECT 
            region,
            CAST(AVG(salary_offer) AS INTEGER) as avg_salary,
            COUNT(*) as offers
        FROM job_listings
        WHERE region != 'Neznamy'
        GROUP BY region
        ORDER BY avg_salary ASC
        LIMIT 5
    """).fetchdf()
    print(bottom5.to_string(index=False))
    
    # 7. Top job titles (pokud existují)
    print("\n[7] TOP 10 nejčastějších pracovních pozic:")
    top_jobs = con.execute("""
        SELECT 
            job_title,
            COUNT(*) as count,
            CAST(AVG(salary_offer) AS INTEGER) as avg_salary
        FROM job_listings
        WHERE job_title IS NOT NULL
        GROUP BY job_title
        ORDER BY count DESC
        LIMIT 10
    """).fetchdf()
    
    if len(top_jobs) > 0:
        print(top_jobs.to_string(index=False))
    else:
        print("  (Žádné job titles nebyly scrapovány)")
    
    # 8. Statistiky job titles
    total_with_titles = con.execute("""
        SELECT COUNT(*) FROM job_listings WHERE job_title IS NOT NULL
    """).fetchone()[0]
    print(f"\n[8] Nabídky s job title: {total_with_titles:,} / {total:,} ({total_with_titles*100/total:.1f}%)")
    
    con.close()
    print("\n" + "="*70)

if __name__ == "__main__":
    try:
        analyze_jobs()
    except Exception as e:
        print(f"[CHYBA] {e}")
        print("Ujistete se, ze existuje data/jobs.duckdb soubor")
