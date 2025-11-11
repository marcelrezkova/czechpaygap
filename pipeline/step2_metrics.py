# pipeline/step2_metrics.py
import pandas as pd
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_OUT = os.getenv("SUPABASE_TABLE_METRICS", "wages_comparison")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("📥 Loading data from Supabase...")
csu = pd.DataFrame(supabase.table("csu_wages").select("*").execute().data)
jobs = pd.DataFrame(supabase.table("job_listings").select("*").execute().data)

# Agregace dat z pracovních nabídek podle regionu a zdroje
print(f"📊 Zpracování {len(jobs)} nabídek z {jobs['source'].nunique() if 'source' in jobs.columns else 1} zdrojů...")

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
    print(f"✅ Uložena agregace podle zdroje: data/wages_by_source.csv")
    
    # Upload dat podle zdrojů do Supabase
    try:
        supabase.table("wages_by_source").upsert(agg_by_source.to_dict(orient="records")).execute()
        print(f"📤 Nahráno {len(agg_by_source)} řádků do Supabase → wages_by_source")
    except Exception as e:
        print(f"⚠️  Chyba při nahrávání wages_by_source: {e}")

# Spojení s ČSÚ daty
merged = csu.merge(agg_total, on="region", how="inner")
merged["pay_gap"] = merged["avg_offer"] - merged["avg_wage"]
merged["pay_gap_pct"] = ((merged["avg_offer"] - merged["avg_wage"]) / merged["avg_wage"] * 100).round(2)

# Seřazení podle pay gap
merged = merged.sort_values("pay_gap", ascending=False)

merged.to_csv("data/wages_comparison.csv", index=False)
print(f"✅ Metriky vypočteny a uloženy: data/wages_comparison.csv")
print(f"\n📊 Statistiky:")
print(f"  - Celkem regionů: {len(merged)}")
print(f"  - Průměrný pay gap: {merged['pay_gap'].mean():.0f} Kč ({merged['pay_gap_pct'].mean():.1f}%)")
print(f"  - Max pay gap: {merged['pay_gap'].max():.0f} Kč v {merged.iloc[0]['region']}")
print(f"  - Min pay gap: {merged['pay_gap'].min():.0f} Kč v {merged.iloc[-1]['region']}")

supabase.table(SUPABASE_OUT).upsert(merged.to_dict(orient="records")).execute()
print(f"\n📤 Uploaded {len(merged)} rows to Supabase → {SUPABASE_OUT}")
