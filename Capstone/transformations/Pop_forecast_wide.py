import dlt
from pyspark.sql.functions import expr, col

@dlt.table(
  name="county_population_forecast_wide",
  comment="Wide-format forecasted population for 2025–2027 with truncated values"
)
def county_population_forecast_wide():
    df = dlt.read("county_population_forecast")

    df = df.withColumn("year", col("year_index") + 2020)
    df = df.withColumn("population", expr("cast(predicted_population as int)"))

    years = [2025, 2026, 2027]
    return df.groupBy("location").pivot("year", years).agg(expr("first(population)"))