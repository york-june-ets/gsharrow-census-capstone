import dlt
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import PandasUDFType
from sklearn.linear_model import LinearRegression
import pandas as pd

# Define schema for output
schema = StructType([
    StructField("location", StringType()),
    StructField("year_index", IntegerType()),
    StructField("predicted_population", DoubleType())
])

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def forecast_poly(pdf: pd.DataFrame) -> pd.DataFrame:
    model = LinearRegression()
    X = pdf[["year_index", "year_squared"]]
    y = pdf["population"]
    model.fit(X, y)

    future = pd.DataFrame({
        "year_index": [5, 6, 7]
    })
    future["year_squared"] = future["year_index"] ** 2
    future["predicted_population"] = model.predict(future[["year_index", "year_squared"]])
    future["location"] = pdf["location"].iloc[0]

    return future[["location", "year_index", "predicted_population"]]

@dlt.table(
  name="county_population_forecast",
  comment="Forecasted population for 2025–2027 using polynomial regression"
)
def county_population_forecast():
    long_df = dlt.read("county_population_long")
    return long_df.groupby("location").apply(forecast_poly)