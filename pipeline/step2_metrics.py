# pipeline/step2_metrics.py
import pandas as pd
import duckdb
import os

print("[LOAD] Loading data from DuckDB...")

# Připojení k DuckDB databázím
csu_db = duckdb.connect("data/csu_data.duckdb", read_only=True)
jobs_db = duckdb.connect("data/jobs.duckdb", read_only=True)

# Načtení dat z časových řad (nejnovější průměrná mzda pro ČR)
timeseries = csu_db.execute("SELECT * FROM wages_timeseries ORDER BY region DESC LIMIT 1").fetchdf()
avg_wage_cz = timeseries['value'].iloc[0]
print(f"[CSU] Celkovy prumer CR z casovych rad: {avg_wage_cz:.0f} Kc")

# Načtení dat z job portálů
jobs = jobs_db.execute("SELECT * FROM job_listings").fetchdf()

# Agregace dat z pracovních nabídek podle regionu a zdroje
print(f"[DATA] Zpracovani {len(jobs)} nabidek z {jobs['source'].nunique() if 'source' in jobs.columns else 1} zdroju...")

# Celková agregace podle regionu
agg_total = jobs.groupby("region").agg(
    avg_offer=("salary_offer", "mean"),
    median_offer=("salary_offer", "median"),
    min_offer=("salary_offer", "min"),
    max_offer=("salary_offer", "max"),
    offers=("salary_offer", "count")
).reset_index()

# Agregace podle regionu a zdroje (pokud existuje sloupec source)
if "source" in jobs.columns:
    agg_by_source = jobs.groupby(["region", "source"]).agg(
        avg_offer=("salary_offer", "mean"),
        offers=("salary_offer", "count")
    ).reset_index()
    agg_by_source.to_csv("data/wages_by_source.csv", index=False)
    print(f"[OK] Ulozena agregace podle zdroje: data/wages_by_source.csv")

# Přidání ČSÚ průměru ke každému regionu
# Prozatím používáme celkový průměr ČR pro všechny regiony
agg_total["avg_wage"] = avg_wage_cz
merged = agg_total.copy()
merged["pay_gap"] = merged["avg_offer"] - merged["avg_wage"]
merged["pay_gap_pct"] = ((merged["avg_offer"] - merged["avg_wage"]) / merged["avg_wage"] * 100).round(2)

# Seřazení podle pay gap
merged = merged.sort_values("pay_gap", ascending=False)

merged.to_csv("data/wages_comparison.csv", index=False)
print(f"[OK] Metriky vypocteny a ulozeny: data/wages_comparison.csv")
print(f"\n[STATS] Statistiky:")
print(f"  - Celkem regionu: {len(merged)}")
print(f"  - Prumerny pay gap: {merged['pay_gap'].mean():.0f} Kc ({merged['pay_gap_pct'].mean():.1f}%)")
print(f"  - Max pay gap: {merged['pay_gap'].max():.0f} Kc v {merged.iloc[0]['region']}")
print(f"  - Min pay gap: {merged['pay_gap'].min():.0f} Kc v {merged.iloc[-1]['region']}")

# Uzavření DuckDB připojení
csu_db.close()
jobs_db.close()
print("\n[OK] DuckDB pripojeni uzavrena")
