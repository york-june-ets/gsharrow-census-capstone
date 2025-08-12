import dlt
from pyspark.sql.functions import col, round

@dlt.table(
  name="county_forecast_combined",
  comment="Joined table with population and AGI forecasts by county, including AGI per capita"
)
def county_forecast_combined():
    pop_df = dlt.read("county_population_change_full")
    agi_df = dlt.read("agi_final")

    # Join on location
    joined_df = pop_df.join(agi_df, on="location", how="inner")

    # Drop unwanted columns
    cleaned_df = joined_df.drop("2025", "2026")

    # Rename columns
    rename_map = {
        "POPESTIMATE2020": "pop_2020",
        "POPESTIMATE2021": "pop_2021",
        "POPESTIMATE2022": "pop_2022",
        "POPESTIMATE2023": "pop_2023",
        "POPESTIMATE2024": "pop_2024",
        "2027": "pop_2027_predicted"
    }

    for old_name, new_name in rename_map.items():
        if old_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumnRenamed(old_name, new_name)

    # Add AGI per capita column
    final_df = cleaned_df.withColumn(
        "AGI_per_capita_2027",
        round(col("AGI_2027_predicted") / col("pop_2027_predicted"), 2)
    )

    return final_df