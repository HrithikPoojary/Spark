from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('Repartition')\
        .master('local[*]')\
        .getOrCreate()


df = spark.read.load(path = '/practice/test/', format = 'csv' , sep = ',' , schema = 'col1 int , col2 int , col3 int')

df_filter = df.where(df.col1<501)


print('Before repartition')

print(df_filter.rdd.glom().map(len).collect())

print('After repartiton')


#repartiton(numPartitons , *cols)
#repatition takes two arguments

df_filter1 = df_filter.repartition(5)

print(df_filter1.rdd.glom().map(len).collect())
