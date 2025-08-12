import dlt
from pyspark.sql.functions import concat_ws, col

Pop_files = "dbfs:/Volumes/ets_thriventcohort/default/population_data"

@dlt.table(
  comment="Population Data",
)
def Pop_raw():
    df = spark.read.option("header", True).csv(Pop_files)

    df = df.withColumn("location", concat_ws(", ", col("CTYNAME"), col("STNAME")))

    df = df.filter(col("location").contains("County"))
    
    return df




    
