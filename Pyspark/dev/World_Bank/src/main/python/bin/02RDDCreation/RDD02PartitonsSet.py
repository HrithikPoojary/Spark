from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('RDD Partitons')\
        .getOrCreate()

rdd = spark.sparkContext.textFile('/practice/sample.txt',minPartitions=5)

print(rdd.getNumPartitions())
print(rdd.glom().map(len).collect())
