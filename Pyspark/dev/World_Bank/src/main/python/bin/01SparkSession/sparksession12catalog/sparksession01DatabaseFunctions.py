from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Python Spark Basic Example')\
        .getOrCreate()


# to get metadata informations

print(spark.catalog.currentDatabase())


spark.sql("CREATE DATABASE IF NOT EXISTS test")

# to set database
spark.catalog.setCurrentDatabase('test')

print(spark.catalog.currentDatabase())

print(spark.catalog.listDatabases())

print(type(spark.catalog.listDatabases()))

print(type(spark.catalog.listDatabases()[0]))

print('------------List compression example------------------------')

print([i*2 for i in range(0,10)])

print('--------To find database is present or not---------')

print('test' in [i.name for i in spark.catalog.listDatabases()])

print('test99999999999' in [i.name for i in spark.catalog.listDatabases()])

spark.stop()
