from pyspark.sql import SparkSession
from pyspark.sql.functions import sequence

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()

df = spark.createDataFrame([(1 ,10) , (2,10)] ,schema = ['dummy1','dummy2'])

df.select(sequence( df.dummy1 , df.dummy2)).show(truncate = False)
