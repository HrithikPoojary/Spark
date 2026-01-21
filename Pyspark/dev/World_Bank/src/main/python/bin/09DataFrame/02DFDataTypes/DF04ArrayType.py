from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.types import StructType,StructField,StringType , IntegerType ,MapType,ArrayType #type:ignore

spark = SparkSession.builder\
	.appName("Type")\
	.master("local[*]")\
	.getOrCreate()

data = [
        ("Luffy" , [123,456,789]),
        ("Zoro" , [567,987,123]),
        ("Nami" , [987,654,321])
]
schema = StructType(
        [
                StructField(name = "Name",dataType = StringType(),nullable = False),
                StructField(name = "MobileNumber", dataType = ArrayType(elementType = IntegerType() , containsNull = False),nullable = True)
        ]
)

df =spark.createDataFrame(data = data ,schema = schema)

df.show(truncate =False)
df.printSchema()

for i in range(3):
        df.select(df.MobileNumber[i]).show(truncate = False)
