
from pyspark.sql import SparkSession #type:ignore
from pyspark.sql import Column,Row #type:ignore
from pyspark.sql.types import StructType,StructField , StringType,IntegerType  #type:ignore
from pyspark.sql.functions import col #type:ignore

spark = SparkSession.builder\
        .appName('Column')\
        .master('local[*]')\
        .getOrCreate()

data = [Row(id = 1 , value = "foo") , Row(id = 2 , value =None)]

df = spark.createDataFrame(data = data)

df.show(truncate=False)
df.printSchema()

# to check is string present or not but for null it will return null so we have to hundle that
df.select(df.value == 'foo').show()

#eqNullSafe to hundle null

df.select(df.value == 'foo' , df.value.eqNullSafe('foo')).show()

#isNull

df.select(df.value.isNull()).show()

# isNotNulll

df.select(df.value.isNotNull()).show()


spark.stop()


