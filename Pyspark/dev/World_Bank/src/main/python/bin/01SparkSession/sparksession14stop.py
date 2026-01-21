from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()

new_spark = spark.newSession()

print(spark)


new_spark.stop()

# spark = new_spark link via sparkContext

df = spark.createDataFrame(data = [('A',)], schema = ('Name string'))

# Will get sc has no attribute like 'None'
df.show()