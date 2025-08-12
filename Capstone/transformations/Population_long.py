import dlt
from pyspark.sql.functions import expr

@dlt.table(
  name="county_population_long",
  comment="Long-format population data from 2020 to 2024"
)
def county_population_long():
    df = dlt.read("pop_cleaned")

    # Use stack to rotate columns into rows
    long_df = df.selectExpr(
        "location",
        "stack(5, " +
        "'2020', POPESTIMATE2020, " +
        "'2021', POPESTIMATE2021, " +
        "'2022', POPESTIMATE2022, " +
        "'2023', POPESTIMATE2023, " +
        "'2024', POPESTIMATE2024" +
        ") as (year, population)"
    ).withColumn("year", expr("cast(year as int)"))
    return long_df.withColumn("year_index", expr("year - 2020")) \
                  .withColumn("year_squared", expr("year_index * year_index"))
