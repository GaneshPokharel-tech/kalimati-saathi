import pandas as pd

df = pd.read_csv("data/processed/kalimati_daily_prices_clean.csv")

print("=== KALIMATI DAILY MARKET SUMMARY ===")
print(f"Total items: {len(df)}")
print()

print("Top 10 most expensive by average price:")
print(df.sort_values("avg_price", ascending=False)[["commodity", "unit", "avg_price"]].head(10).to_string(index=False))
print()

print("Top 10 cheapest by average price:")
print(df.sort_values("avg_price", ascending=True)[["commodity", "unit", "avg_price"]].head(10).to_string(index=False))
print()

df["price_spread"] = df["max_price"] - df["min_price"]
print("Top 10 widest price spread:")
print(df.sort_values("price_spread", ascending=False)[["commodity", "unit", "min_price", "max_price", "price_spread"]].head(10).to_string(index=False))
