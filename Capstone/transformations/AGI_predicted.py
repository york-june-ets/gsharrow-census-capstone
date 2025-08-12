import dlt
from pyspark.sql.functions import col, lit, expr, floor

@dlt.table(
  name="county_agi_forecasted",
  comment="County-level AGI with predicted value for 2027"
)
def county_agi_forecasted():
    df = dlt.read("county_agi_joined")

    # Reshape to long format for regression
    df_long = df.selectExpr(
        "location",
        "AGI_2020 as AGI_2020", "AGI_2021 as AGI_2021", "AGI_2022 as AGI_2022"
    ).selectExpr(
        "location", "stack(3, 2020, AGI_2020, 2021, AGI_2021, 2022, AGI_2022) as (year, AGI)"
    ).filter(col("AGI").isNotNull())

    # Compute regression coefficients per location
    df_regression = df_long.groupBy("location").agg(
        expr("covar_samp(year, AGI) / var_samp(year) as slope"),
        expr("avg(AGI) - (covar_samp(year, AGI) / var_samp(year)) * avg(year) as intercept")
    )

    # Predict AGI for 2027
    df_predicted = df_regression.withColumn(
        "AGI_2027_predicted", floor(col("slope") * lit(2027) + col("intercept"))
    )

    # Join back to original wide table
    df_joined = dlt.read("county_agi_joined").join(df_predicted.select("location", "AGI_2027_predicted"), on="location", how="left")

    return df_joined
