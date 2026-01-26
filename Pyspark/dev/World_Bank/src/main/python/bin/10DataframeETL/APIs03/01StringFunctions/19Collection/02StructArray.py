from pyspark.sql  import SparkSession
from pyspark.sql.functions import col,struct , array

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


emp3 = spark.createDataFrame(data = [  ('John' , [10,20,20] , [25,11,10]) , 
				       ('Robert',[15,13,55],[5,None ,29]) ,
					('James',[11,13,45],[5,89,79] )  ] , schema = ['empname' , 'score_arr1' , 'score_arr2'])

#emp3.show()

df = spark.sql("select array(struct(1,'a') , struct(2,'b')) as data")

print('---------struct--------')
#struct(*col)
demo = emp1.select(struct(emp1.firstname , emp1.lastname).alias('empname'))
demo.printSchema()
demo.select(demo.empname.firstname ,demo.empname.lastname).show()


print('---------array----------')
#array(*col)
demo = emp2.select(emp2.score1 , emp2.score2 , emp2.score3,array(emp2.score1 , emp2.score2 , emp2.score3).alias('emp_array'))
demo.printSchema()
demo.show()



