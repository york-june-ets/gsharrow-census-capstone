import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="county_AGI_2020_cleaned",
  comment="Cleaned county metrics with AGI column and casted types"
)
def county_AGI_2020_cleaned():
    df = dlt.read("county_AGI_2020_Metrics")

    return df.select(
        col("location"),
        col("A00100").cast("double").alias("AGI_2020"),
    )
