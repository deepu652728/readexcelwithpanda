import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Real-Time Data Pipeline Started ...")

spark = SparkSession.builder.appName("pandaExcel").getOrCreate()

print("Spark Session Created")

df = pd.read_excel(r"C:/Users/admin/Desktop/Book1.xlsx")
print(df)
schema = StructType(
    [
        StructField("Parties", StringType()),
        StructField("Email id", StringType())

    ]
)
df1 = spark.createDataFrame(df, schema)
print(df1.show())
