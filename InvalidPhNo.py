from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 2.	How to Eliminate Invalid Contact Number using Scala/Python ?
spark = SparkSession.builder.appName("Qtn").getOrCreate()

# 2.	How to Eliminate Invalid Contact Number using Scala/Python ?
# create dataframe
Col_Name = ["ID", "Name", "ContactNo"]
Data = [(100, "xx", "9999999999"), (100, "YYY", "8888888888"),
        (100, "ZZZ", "8ag65jgjv898"), (100, "AAA", "555555ddf5555")]
df = spark.createDataFrame(Data, Col_Name)
df1 = df.withColumn("New_Contact", F.regexp_replace(
    df['ContactNo'], r"(\D)", ""))