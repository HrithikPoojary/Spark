from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('Read')\
        .getOrCreate()


# to check globalTempView table
spark.sql('show tables in global_temp').show()
# to check normal tempView
spark.sql('show tables')

lst = [('Luffy' , 19) , ('Zoro' , 21)]

df = spark.createDataFrame(data = lst , schema = ['Name' , 'Age'])

df.createOrReplaceTempView('Pirates')

print('----------------Normal spark.sql---------------')
spark.sql('select * from pirates').show()

print('----------------spark.table-----------------')
spark.table('pirates').show()





spark.stop()
