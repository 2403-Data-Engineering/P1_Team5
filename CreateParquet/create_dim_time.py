
from pathlib import Path
import pandas as pd


OUT_DIR = Path("powerbi_exports")
OUT_DIR.mkdir(exist_ok=True)

# PaySim has 744 hourly steps = 30 days * 24 hours
steps = list(range(1, 745))

dim_time = pd.DataFrame({"step": steps})

# Derived time fields based on PaySim's hourly step counter
dim_time["derived_hour"] = (dim_time["step"] - 1) % 24
dim_time["derived_day"] = ((dim_time["step"] - 1) // 24) + 1
dim_time["derived_week"] = ((dim_time["derived_day"] - 1) // 7) + 1

#readable datetime using an artificial base date for dashboarding
base_datetime = pd.Timestamp("2024-01-01 00:00:00")
dim_time["datetime"] = dim_time["step"].apply(
    lambda s: base_datetime + pd.Timedelta(hours=s - 1)
)

dim_time["date"] = dim_time["datetime"].dt.date
dim_time["day_of_week"] = dim_time["datetime"].dt.day_name()
dim_time["datetime"] = dim_time["datetime"].astype(str)

output_path = OUT_DIR / "dim_time.parquet"
dim_time.to_parquet(output_path, index=False)

print(f"dim_time exported to {output_path.resolve()}")
print(dim_time.head())
