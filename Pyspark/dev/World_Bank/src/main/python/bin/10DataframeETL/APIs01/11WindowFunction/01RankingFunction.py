from pyspark.sql import SparkSession 
from pyspark.sql.window import  Window
from pyspark.sql import window
from pyspark.sql.functions import row_number,rank,dense_rank,percent_rank,ntile,cume_dist


spark = SparkSession.builder\
	.appName('Window')\
	.master('local[*]')\
	.getOrCreate()

#print(help(window))

data = [
    ('James' , 'Sales','NY' , 9000 , 34),
    ('Alicia' , 'Sales','NY' , 8600 , 56),
    ('Robert' , 'Sales','CA' , 8100 , 30),
    ('John' , 'Sales', 'AZ' , 8600 , 31),
    ('Rahul' , 'Sales', 'AZ' , 1000 , 31),
    ('Ross' , 'Sales','AZ' , 8100 , 33),
    ('Lisa' , 'Finance','CA' , 9000 , 24),
    ('Deja' , 'Finance','CA' , 9900 , 40),
    ('Sugie' , 'Finance','NY' , 7900 , 36),
    ('Charlie' , 'Finance','AZ' , 5000 , 36),
    ('Ram' , 'Finance','NY' , 8900 , 53),
    ('Kyle' , 'Marketing','CA' , 8000 , 25),
    ('Reid' , 'Marketing','NY' , 9100 , 50)
]

schema = ['empname', 'dept','state','salary','age']

df = spark.createDataFrame(data = data ,schema = schema)


spec = Window.partitionBy(df.dept).orderBy(df.salary.desc())
print('------Window spec object----------')
print(spec)

df.select(df.dept , df.salary).withColumn("Row_Nubmer" , row_number().over(spec))\
 .withColumn("Rank" , rank().over(spec))\
 .withColumn("Dense_Rank" , dense_rank().over(spec))\
 .withColumn("Percent_Rank" , percent_rank().over(spec))\
 .withColumn("Ntile" , ntile(3).over(spec))\
 .withColumn("Cummulative_Distribution" , cume_dist().over(spec))\
 .show()

# percent_rank ->(srart 0 ends 0) Finance 5 rows
#		 0/4  -> 0
#		 1/4   -> 0.25
#		 2/4   -> 0.5
#		 3/4   -> 0.75
#		 4/4   -> 1.0
#              -> Sales 6 rows one duplicate
#		 0/5 -> 0        Before this record 0
#                1/5 -> 0.2      Before this record 1
#                1/5 -> 0.2      Before this record 1
#		 3/5 -> 0.6      Before this record 3
#		 3/5 -> 0.6      Before this record 3
#                5/5 -> 1.0      Before this record 5
# ntile -> divide the records evenly 
#	   16 records  ntile(4)
#	   First   1-4   rows -> 1
#	   Second  5-8   rows -> 2
#	   Third   9-12  rows -> 3
#	   Four    12-16 rows -> 4
# cume_ist -> Finance 5 records 
#		1  -> 1/5 =  0.2
#		2  -> 2/5 =  0.4
#		3  -> 3/5 =  0.6
#		4  -> 4/5 =  0.8
#		5  -> 5/5 =  1.0
#            Sales 6 records duplicates
#		1 -> 1/6 = 1.66..
#		2 -> 3/6 = 0.5
#		3 -> 3/6 = 0.5
#		4 -> 5/6 = 0.83..
#		5 -> 5/6 = 0.83..
#		6 -> 6/6 = 1.0
spark.stop()















