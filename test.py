from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, count, coalesce
spark = SparkSession.builder.appName("x").getOrCreate()
col = StructType([StructField(name="team1", dataType=StringType(), nullable=True),
                 StructField(name="team2", dataType=StringType(),
                             nullable=True),
                 StructField(name="won", dataType=StringType(), nullable=True)])
data = [('IND', 'PAK', 'IND'), ('AUS', 'IND', 'IND'), ('PAK', 'ENG', 'ENG'),
        ('ENG', 'IND', 'IND'), ('PAK', 'AUS', 'AUS'), ('IND', 'AUS', 'IND')]

df = spark.createDataFrame(data, col)
df1 = df.select('team1').unionAll(df.select('team2'))
df2 = df1.groupBy('team1').agg(
    coalesce(count('team1'), F.lit(0)).alias("total_no_of_match"))
df3 = df.groupBy("won").agg(count("won").alias("woncount"))
df2.join(df3, df2.team1 == df3.won, how="left_outer").select(df2.team1,
                                                             df2.total_no_of_match, df3.woncount, df2.total_no_of_match-df3.woncount).show()
