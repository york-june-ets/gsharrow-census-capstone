import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="county_forecast_combined",
  comment="Joined table with population and AGI forecasts by county"
)
def county_forecast_combined():
    pop_df = dlt.read("county_population_change_full")
    agi_df = dlt.read("agi_final")

    # Join on location
    joined_df = pop_df.join(agi_df, on="location", how="inner")

    # Drop unwanted columns
    cleaned_df = joined_df.drop("2025", "2026")

    # Reorder columns: location first
    location_col = col("location")
    other_cols = [col(c) for c in cleaned_df.columns if c != "location"]

    return cleaned_df.select(location_col, *other_cols)
