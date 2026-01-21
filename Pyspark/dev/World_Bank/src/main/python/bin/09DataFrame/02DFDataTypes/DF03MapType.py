from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.types import StructType,StructField,StringType , IntegerType ,MapType #type:ignore

spark = SparkSession.builder\
	.appName("Type")\
	.master("local[*]")\
	.getOrCreate()


data = [ 
	 ("Luffy" , {"Position" : "Captain" ,
		     "Bounty" : "3B"} ) , 
	 ("Zoro" , {"Position" : "SwordsMan" ,
		    "Bounty" : "1.1B"} )
       ]

schema = StructType ([
StructField(name = "Name" , dataType = StringType() , nullable = True),
StructField(name = "Properties" , dataType = MapType(keyType = StringType() , valueType = StringType() , valueContainsNull = True) , nullable = True)
])

df = spark.createDataFrame(data = data , schema = schema)

df.show(truncate = False)
df.printSchema()

df.select(df.Properties).show(truncate=False)
df.select(df.Properties['Position'],df.Properties['Bounty']).show(truncate = False) 

