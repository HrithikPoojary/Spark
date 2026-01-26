from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()

data = [('Luffy' , 80 ,10) , ('Zoro' , None , 5) , ('Nami' , 50 ,50) ,(None , None  , None) , ('Robert' , 30 ,35)]
schema =  'name string , age int , height int'
df = spark.createDataFrame(data = data , schema = schema)

df.show()


#df.na.fill(value , subset= None)
# fill(50)  only non-string column filled
# fill('ram') only string column filled
print('------df.na.fill(50)-------')
df.na.fill(50).show()

print('----df.na.fill("Ram")-------')
df.na.fill("Ram").show()

print('----df.na.fill({"age":50})---------')
df.na.fill({"age":50}).show()

print('----df.na.fill({"age":50 , "name":"Ram"})---------')
df.na.fill({"age":50 , "name" : 'Ram'}).show()
