import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="county_agi_joined",
  comment="Joined AGI data from 2020, 2021, and 2022 by location"
)
def county_agi_joined():
    df_2020 = dlt.read("county_agi_2020_cleaned")
    df_2021 = dlt.read("county_agi_2021_cleaned")
    df_2022 = dlt.read("county_agi_2022_cleaned")

    # Join sequentially on location
    joined_df = df_2020 \
        .join(df_2021, on="location", how="inner") \
        .join(df_2022, on="location", how="inner")

    return joined_df
