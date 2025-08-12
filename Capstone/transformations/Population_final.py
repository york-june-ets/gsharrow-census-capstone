import dlt
from pyspark.sql.functions import col, round

@dlt.table(
  name="county_population_change_full",
  comment="Full joined table with population change metrics"
)
def county_population_change_full():
    forecast_df = dlt.read("county_population_forecast_wide")
    cleaned_df = dlt.read("pop_cleaned")

    # Join on location
    joined_df = cleaned_df.join(forecast_df, on="location", how="inner")

    # Add change metrics
    return joined_df.withColumn(
        "pop_change_raw", col("2027") - col("POPESTIMATE2022")
    ).withColumn(
        "pop_change_pct", round((col("2027") - col("POPESTIMATE2022")) / col("POPESTIMATE2022") * 100, 2)
    )
