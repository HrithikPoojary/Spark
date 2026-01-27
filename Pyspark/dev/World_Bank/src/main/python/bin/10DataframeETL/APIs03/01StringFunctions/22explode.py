from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,explode_outer,posexplode ,posexplode_outer


spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()


data = [('Alicia',['Java','Scala'],{'hair':'black','eye':'brown'}),
	('Robert',['Spark','Java',None],{'hair':'brown','eye':None}),
	('Mike',['CSharp',''],{'hair':'red','eye':''}),
	('John',None,None),
	('Jeff',['1','2'],{})]

schema = ['empname' , 'langueage' , 'properties']

emp = spark.createDataFrame(data=data,schema = schema)
emp.show()

print('---------explode on array column------------')
#explode(array) -> convert each array value into rows
#ignore the null row not a null value rows
emp.select(emp.empname,explode(emp.langueage)).show()
print('----------explode on map column---------------')
#explode on map
#convert each key and value into new column
#if it has 2 or more key value pair it will create a new row for that
emp.select(emp.empname , explode(emp.properties)).show()

#explode_outer -> nulls not be skipped
print('-----explode_outer(nulls included) on arrays---------')
emp.select(emp.empname , explode_outer(emp.langueage)).show()
print('------explode_outer on map----------------')
emp.select(emp.empname,explode_outer(emp.properties)).show()


#posexplode  # similar to explode ignore nulls but additionally we get index column
print('------poseexplode on array----------')
emp.select(emp.empname,posexplode(emp.langueage)).show()

print('----posexplode on map-----------')
emp.select(emp.empname , posexplode(emp.properties)).show()


#posexplode_outer -> includes null
print('------poseexplode_outer on array----------')
emp.select(emp.empname,posexplode_outer(emp.langueage)).show()

print('----posexplode_outer on map-----------')
emp.select(emp.empname , posexplode_outer(emp.properties)).show()

#explode function
emp.select(emp.empname,explode(emp.langueage),explode(emp.properties)).show()


















