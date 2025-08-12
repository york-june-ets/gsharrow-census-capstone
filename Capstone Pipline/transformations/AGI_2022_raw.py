import dlt
from pyspark.sql.functions import regexp_extract, col, create_map, lit, concat_ws
from itertools import chain

# Dictionary mapping state abbreviations to full names
state_map = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming"
}

# Convert dictionary to Spark map expression
mapping_expr = create_map([lit(x) for x in chain(*state_map.items())])

@dlt.table(
  name="county_AGI_2022_Metrics",
  comment="County metrics with full state names and location column"
)
def county_AGI_metrics():
    # Read all CSVs from the folder
    df = spark.read.option("header", True).csv("/Volumes/ets_thriventcohort/default/agi_data/AGI_2022.csv")

    # Extract year from filename using _metadata.file_path
    df = df.withColumn("year", regexp_extract(col("_metadata.file_path"), r"(\d{4})", 1).cast("int"))

    # Map abbreviation to full state name
    df = df.withColumn("state_name", mapping_expr[col("STATE")])

    # Create location column: "County, State"
    df = df.withColumn("location", concat_ws(", ", col("COUNTYNAME"), col("state_name")))

    # Filter rows where location contains "County"
    df = df.filter(col("location").contains("County"))

    return df