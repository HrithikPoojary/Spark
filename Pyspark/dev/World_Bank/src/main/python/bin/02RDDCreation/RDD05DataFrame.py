from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Parallelize')\
        .master('local[2]')\
        .getOrCreate()

df = spark.createDataFrame(data= [('Luffy',18),('Zoro',20)] , schema = ('Name','age'))

# In DF
#print(df.show()) this will print result + None
df.show()
df.printSchema()

rdd= df.rdd

for i in rdd.take(2):
        print(i)