from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()

data = [('Luffy' , 80 ,10) , ('Zoro' , None , 5) , ('Nami' , 50 ,50) ,(None , None  , None) , ('Robert' , 30 ,35)]
schema =  'name string , age int , height int'
df = spark.createDataFrame(data = data , schema = schema)

df.show()
#df.na.drop(how = 'any' , thresh = None , subset = None)
#any or all
print('-----how = any-----')
df.na.drop().show()

print('-----how = all--------')
df.na.drop(how = 'all').show()

#thresh - >  not null columns
print('-------thresh-------')

#1row - thresh = 3
#2row - thresh = 2
#3row - thresh = 3
#4row - thresh = 0
#5row - thresh = 3
df.na.drop(thresh = 2).show()
df.na.drop(thresh = 3).show()
df.na.drop(thresh = 4).show()

print('-------subset-----------')
df.na.drop(subset = 'height').show()
df.na.drop(subset = 'age').show()
