import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="county_AGI_2021_cleaned",
  comment="Cleaned county metrics with AGI column and casted types"
)
def county_AGI_2021_cleaned():
    df = dlt.read("county_AGI_2021_Metrics")

    return df.select(
        col("location"),
        col("A00100").cast("double").alias("AGI_2021"),
    )
