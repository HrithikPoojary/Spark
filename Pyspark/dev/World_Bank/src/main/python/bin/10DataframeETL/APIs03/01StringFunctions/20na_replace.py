from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()

data = [('Luffy' , 80 ,10) , ('Zoro' , None , 5) , ('Nami' , 50 ,50) ,(None , None  , None) , ('Robert' , 30 ,35)]
schema =  'name string , age int , height int'
df = spark.createDataFrame(data = data , schema = schema)

df.show()

#df.na.replace(to_replace , value , subset = None)
#df.na.replace(dict,subset)
print('--------50->99----------')
df.na.replace(50,99).show()
print('------dict replace 50->99-------')
df.na.replace({50:99}).show()

print('-----subset---------')
df.na.replace({"Luffy":"Kaido","Zoro":"King"} , subset = "name").show()
