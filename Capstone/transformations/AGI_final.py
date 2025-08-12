import dlt
from pyspark.sql.functions import col, round

@dlt.table(
  name="agi_final",
  comment="Full joined table with population change metrics"
  )
def agi_changes():
    df = dlt.read("county_agi_forecasted")

    return df.withColumn(
        "AGI_change_raw", col("AGI_2027_predicted") - col("AGI_2022")
    ).withColumn(
        "AGI_change_pct", round((col("AGI_2027_predicted") - col("AGI_2022")) / col("AGI_2022") * 100, 2)
    )