import pandas as pd

csu = pd.read_csv("data/csu_wages.csv")
jobs = pd.read_csv("data/job_listings.csv")

agg = jobs.groupby("region").agg(
    avg_offer=("salary_offer", "mean"),
    offers=("salary_offer", "count")
).reset_index()

merged = csu.merge(agg, on="region", how="inner")
merged["pay_gap"] = merged["avg_offer"] - merged["avg_wage"]

merged.to_csv("data/wages_comparison.csv", index=False)
print("Výpočet rozdílů dokončen")