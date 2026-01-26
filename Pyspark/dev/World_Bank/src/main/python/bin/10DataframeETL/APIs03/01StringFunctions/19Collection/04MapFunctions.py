from pyspark.sql  import SparkSession
from pyspark.sql.functions import create_map,map_from_entries,map_from_arrays,map_keys , map_values , map_concat

spark = SparkSession.builder\
        .appName("Date")\
        .master('local[*]')\
        .getOrCreate()


data = [  ('Alicia' , 'Joseph' , ['Java' , 'Scala' , 'Spark'] , {'hair' : 'black' , 'eye':'brown'}),
          ('Robert' , 'Gee' , ['Spark' , 'Java'] , {'hair':'brown' , 'eye':None} ) ,
          ( 'Mike' ,'Bianca' , ['CSharp' , ''] , {'hair' : 'red' , 'eye' : ''}   ) ,
          ( 'John' ,  'Kumar' , None , None  ) ,
          ( 'Jeff' ,'L' , ['1','2'] , {} )
    ]

schema = ['firstname' , 'lastname' , 'langueage' , 'properties']

emp1 = spark.createDataFrame(data = data , schema = schema)

#emp1.show(truncate = False)


emp2 = spark.createDataFrame(data = [('Robert' , 35 ,40,40 ),
                                     ('Ram'    , 31 ,33,29 ),
                                     ('John'   , 95,89 ,91) ] \
                                        , schema =['name' , 'score1' , 'score2' , 'score3'])
#emp2.show()
emp3 = spark.createDataFrame(data = [  ('John' , [10,21,20] , [25,11,10]) ,
                                       ('Robert',[15,13,55],[5, 79,13]) ,
                                        ('James',[11,13,45],[5,89,79] )  ] , schema = ['empname' , 'score_arr1' , 'score_arr2'])



df = spark.sql("select array(struct(1,'a') , struct(2,'b')) as data")
df2 = spark.createDataFrame(data = [( {'a': 1} ,{'b':2 } ,) ] ,schema = ['dummy1' , 'dummy2' , ])
#df2.show()


print('--------create_map----------------')
demo = emp1.select(emp1.firstname , emp1.lastname , create_map(emp1.firstname , emp1.lastname))
demo.show(truncate =False)
demo.printSchema()

df.printSchema()
#struct of array
print('---------map_from_entries-------------')
demo =df.select(map_from_entries(df.data).alias("data"))
demo.printSchema()
demo.show(truncate= False)
demo.select(demo.data['1']).show()


print('---------map_from_array---------------')
demo = emp3.select(emp3.score_arr1 , emp3.score_arr2 , map_from_arrays(emp3.score_arr1 , emp3.score_arr2).alias("mapfromarray"))
demo.show(truncate=False)
demo.select(demo.mapfromarray['13']).show(truncate = False)

print('--------map_keys-----------------')
emp1.select(emp1.properties,map_keys(emp1.properties)).show(truncate = False)

print('--------map_values-------------')
emp1.select(emp1.properties,map_values(emp1.properties)).show(truncate = False)

print('-------map_concat------------')
df2.select(df2.dummy1 ,df2.dummy2,map_concat(df2.dummy1 , df2.dummy2)).show(truncate = False)

