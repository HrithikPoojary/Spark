from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('Repartition')\
        .master('local[*]')\
        .getOrCreate()


#repartiton(numPartitons , *cols)
#repatition takes two arguments

data = [('Ram' ,30) , ('Raj',25) , ('James' , 30) , ('Joann' , 25) , ('Kyle',25),('Robert',30) , ('Reid',35) , ('Sam' ,35)]

df = spark.createDataFrame(data = data , schema = ['name' , 'age'])

df.show()


# repartition('col').rdd.getNumPartitions() == spark.conf.get("spark.sql.shuffle.parttions")
# repartition(3 , 'col') this will consider only  first parameter that is 3

print(f'Before partition {df.rdd.getNumPartitions()}')

df = df.repartition('age')

print(f'After partition {df.rdd.getNumPartitions()}')

print(f'Default shuffle setting {spark.conf.get("spark.sql.shuffle.partitions")}')

spark.conf.set("spark.sql.shuffle.partitions" , 100)

spark.stop()
