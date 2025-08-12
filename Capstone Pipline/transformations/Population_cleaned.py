import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="pop_cleaned",
  comment="Cleaned population data"
)
def Pop_cleaned():
    df = dlt.read("Pop_raw")

    return df.select(
        col("location"),
        col("POPESTIMATE2020"),
        col("POPESTIMATE2021"),
        col("POPESTIMATE2022"),
        col("POPESTIMATE2023"),
        col("POPESTIMATE2024"),

    )
