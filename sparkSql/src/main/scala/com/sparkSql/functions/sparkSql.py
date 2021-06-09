import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("read_and_write_hdfs").master("local").getOrCreate()
sc =  spark.sparkContext
empSchema = StructType([StructField("EMPNO",IntegerType(),True),
    StructField("ENAME",StringType(),True),
    StructField("JOB",StringType(),True),
    StructField("MGR",IntegerType(),True),
    StructField("HIREDATE",StringType(),True),
    StructField("SAL",IntegerType(),True),
    StructField("COMM",IntegerType(),True),
    StructField("DEPTNO",IntegerType(),True)])
  
deptSchema = StructType([StructField("DEPTNO",IntegerType(),True),
    StructField("DNAME",StringType(),True),
    StructField("LOC",StringType(),True) ])
salGradeSchema = StructType([StructField("GRADENUMBER",IntegerType(),True),
    StructField("LOWSAL",IntegerType(),True),
    StructField("HISAL",IntegerType(),True) ])

empDF = spark.read.option("delimiter","|").schema(empSchema).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\emp.txt")
deptDF = spark.read.option("delimiter","|").schema(deptSchema).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\dept.txt")
salgradeDF = spark.read.option("delimiter","|").schema(salGradeSchema).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\SalGrade.txt")
empDF.createOrReplaceTempView("emp")
deptDF.createOrReplaceTempView("dept")
salgradeDF.createOrReplaceTempView("salgrade")

def order_of_execution():
   spark.sql("""select max(sal),min(sal) from emp """).show(truncate=False)

class string:
    def replace_translate(self):
      spark.sql("""select replace(lower(ename),'mi','xx') from emp """).show(truncate=False)
      spark.sql("""select translate(lower(ename),'mi','xx') from emp """).show(truncate=False)
      spark.sql("""select regexp_replace(lower(ename),'[a-i]','x') from emp """).show(truncate=False)
    
    def extract(self):
      spark.sql("""select regexp_extract('anil kumar','(a.*)' ) from emp """).show(truncate=False)

    def caseconversion(self):
      spark.sql("""select  lower(ename) from emp """).show(truncate=False)
      spark.sql("""select  upper(ename) from emp """).show(truncate=False)

    def sha2(self):
      spark.sql("""select  sha2(ename,256) from emp """).show(truncate=False)

    
    def subStringByChar(self):
      spark.sql("""select  substr(ename,1,1) from emp """).show(truncate=False)
      spark.sql("""select  substr(ename,0,1) from emp """).show(truncate=False)
      spark.sql("""select  substr(ename,-1,1) from emp """).show(truncate=False)
      spark.sql("""select  substring(ename,1,1) from emp """).show(truncate=False)
      spark.sql("""select  substring(ename,0,1) from emp """).show(truncate=False)
      spark.sql("""select  substring(ename,-1,1) from emp """).show(truncate=False)
      spark.sql("""select  substring_index("anil|kumar|kan","|",1) from emp """).show(truncate=False)
      spark.sql("""select  substring_index("anil|kumar|kan","x",1) from emp """).show(truncate=False)
      spark.sql("""select  substring_index("anil|kumar|kan","|",2) from emp """).show(truncate=False)

    
    def index(self):
      spark.sql("""select   instr("anil|kumar|kan","|") from emp """).show(truncate=False)
      spark.sql("""select   instr("anil|kumar|kan","x") from emp """).show(truncate=False)

    
    def md5(self):
      spark.sql("""select   md5("anil|kumar|kan" ) from emp """).show(truncate=False)
      spark.sql("""select   md5(" " ) from emp """).show(truncate=False)

    
    def reverse(self):
      spark.sql("""select reverse(" anil|kumar|kan" ) from emp """).show(truncate=False)
    
    def nvl(self):
      spark.sql("""select nvl(comm,0 ) from emp """).show(truncate=False)
    
    def lengthCount(self):
      spark.sql("""select length(ename) from emp """).show(truncate=False)
      spark.sql("""select count(ename) from emp """).show(truncate=False)
class number:
    def arthematic(self):
      spark.sql("""select 5+4 """).show(truncate=False)
      spark.sql("""select -5+4 """).show(truncate=False)
      spark.sql("""select 5*4 """).show(truncate=False)
      spark.sql("""select 5/4 """).show(truncate=False)

    
    def abs(self):
      spark.sql("""select abs(5/4) """).show(truncate=False)
      spark.sql("""select abs(-5+4) """).show(truncate=False)
    
    def floor_ceiling_round(self):
      spark.sql("""select floor(1.231) """).show(truncate=False)
      spark.sql("""select ceiling(1.231) """).show(truncate=False)
      spark.sql("""select round(1.231, 2) """).show(truncate=False)
      spark.sql("""select floor(-1.231) """).show(truncate=False)
      spark.sql("""select ceiling(-1.231) """).show(truncate=False)
      spark.sql("""select round(-1.231, 2) """).show(truncate=False)
    
    def mod(self):
      spark.sql("""select mod(4,2) """).show(truncate=False)
      spark.sql("""select mod(-4,2) """).show(truncate=False)
      spark.sql("""select mod(2,4) """).show(truncate=False)
      spark.sql("""select mod(5,2) """).show(truncate=False)
    
    def power(self):
      spark.sql("""select power(-4,2) """).show(truncate=False)
      spark.sql("""select power(-4,3) """).show(truncate=False)
    
    def cbrt_sqrt(self):
      spark.sql("""select sqrt(5) """).show(truncate=False)
      spark.sql("""select cbrt(5) """).show(truncate=False)
class date:

    def add_month(self):
       
       spark.sql("""select add_months("1982-01-23",1) """).show(truncate=False)
       spark.sql("""select add_months("1982-01-23",-1)""").show(truncate=False)

    def  date_add(self):
      spark.sql("""select date_add("1982-01-23",1) """).show(truncate=False)
      spark.sql("""select date_add("1982-01-23",-1) """).show(truncate=False)
    
    def date_subtract(self):
      spark.sql("""select date_sub("1982-01-23",1) """).show(truncate=False)
      spark.sql("""select date_sub("1982-01-23",-1) """).show(truncate=False)

    
    def datediff(self):
      spark.sql("""select datediff("1982-01-23","1982-01-23") """).show(truncate=False)
      spark.sql("""select datediff("1982-01-23","2020-01-23") """).show(truncate=False)
    
    def now_currentDate(self):
      spark.sql("""select  now() """).show(truncate=False)
      spark.sql("""select current_date() """).show(truncate=False)
class dataTypeCast:
    def int_to_string(self):
      spark.sql("""select cast(sal as string) from emp""").show(truncate=False)
  
    def string_to_integer(self):
      spark.sql("""select cast(sal as int) from emp""").show(truncate=False)

    
    def string_to_date(self):
      spark.sql("""select to_date(HIREDATE,"dd-MMM-yyyy") from emp""").show(truncate=False)

    
    def string_to_decimal(self):
      spark.sql("""select cast(empno as decimal(19,2)) from emp""").show(truncate=False)

    
    def string_to_array(self):
      spark.sql("""select split(ename,"") from emp""").show(truncate=False) 
        
    def array_to_string(self):
      
      spark.sql("""select array_join(split(ename,""),"|"," ") from emp""").show(truncate=False)
class window:

    #we can use both partition by and order by in all widow function
   #source_exp
    def first_value(self):
      spark.sql("""select empno,deptno,sal,first_value(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,first_value(sal) over() from emp""" ).show(truncate=False)
    
    def lase_value(self):
      spark.sql("""select empno,deptno,sal,last_value(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,last_value(sal) over() from emp""" ).show(truncate=False)

    
    def sum(self): 
      spark.sql("""select empno,deptno,sal,sum(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,sum(sal) over() from emp""" ).show(truncate=False)

    
    def avg(self): 
      spark.sql("""select empno,deptno,sal,avg(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,avg(sal) over() from emp""" ).show(truncate=False)

    
    def count(self): 
      spark.sql("""select empno,deptno,sal,count(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,count(sal) over() from emp""" ).show(truncate=False)

    
    def max(self): 
      spark.sql("""select empno,deptno,sal,max(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,max(sal) over() from emp""" ).show(truncate=False)

    
    def min(self): 
      spark.sql("""select empno,deptno,sal,min(sal) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,min(sal) over() from emp""" ).show(truncate=False)

    

    #source_exp + order by
    def lead(self):
      spark.sql("""select empno,deptno,sal,sal+lead(sal,1,null) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,sal+lead(sal,1,null) over(order by sal) from emp""" ).show(truncate=False)

     #number of rows and default
    def lag(self):
      spark.sql("""select empno,deptno,sal,sal-lag(sal,1,null) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,sal-lag(sal,1,null) over(order by sal) from emp""" ).show(truncate=False)

    
    def ntile(self): 
      spark.sql("""select empno,deptno,sal,ntile(3) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,ntile(5) over(order by sal) from emp""" ).show(truncate=False)
    


    #order by
    def cume_dist(self):
      spark.sql("""select empno,deptno,sal,cume_dist() over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,cume_dist() over(order by sal) from emp""" ).show(truncate=False)

    
    def dense_rank(self):
      spark.sql("""select empno,deptno,sal,DENSE_RANK() over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,DENSE_RANK() over(order by sal) from emp""" ).show(truncate=False)

    
    def percent_rank(self): 
      spark.sql("""select empno,deptno,sal,percent_rank() over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,percent_rank() over(order by sal) from emp""" ).show(truncate=False)
    
    def rank(self): 
      spark.sql("""select empno,deptno,sal, rank() over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal, rank() over(order by sal) from emp""" ).show(truncate=False)
    
    def row_number(self): 
      spark.sql("""select empno,deptno,sal,row_number() over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(truncate=False)
      spark.sql("""select empno,deptno,sal,row_number() over(order by sal) from emp""" ).show(truncate=False)
class groupby:
     def dept_wise_group(self):
       spark.sql("""select deptno,count(deptno) from emp group by deptno""").show(truncate=False)

     def filter_on_gropy(self):
       spark.sql("""select deptno,count(deptno) from emp group by deptno having count(deptno) <= 4""").show(truncate=False)
class where :
    def comparision(self):
      spark.sql("""select * from emp where deptno = 10""").show(truncate=False)
      spark.sql("""select * from emp where deptno <> 10""").show(truncate=False)
      spark.sql("""select * from emp where deptno < 10""").show(truncate=False)
      spark.sql("""select * from emp where deptno > 10""").show(truncate=False)
      spark.sql("""select * from emp where deptno <= 10""").show(truncate=False)
      spark.sql("""select * from emp where deptno >= 10""").show(truncate=False)

    
    def like(self):
      spark.sql("""select * from emp where deptno like "1%" """).show(truncate=False)
      spark.sql("""select * from emp where deptno like "_0" """).show(truncate=False)
      spark.sql("""select * from emp where deptno not like "1%" """).show(truncate=False)
      spark.sql("""select * from emp where deptno not like "_0" """).show(truncate=False)
    
    def in_01(self):
      spark.sql("""select * from emp where deptno in (10,20) """).show(truncate=False)
      spark.sql("""select * from emp where deptno not in (10,20) """).show(truncate=False)
    
    def between(self):
      spark.sql("""select * from emp where deptno between 11 and 29  """).show(truncate=False)
      spark.sql("""select * from emp where deptno  not between 21 and 30   """).show(truncate=False)
     
    def multipleCondition(self):
      spark.sql("""select * from emp where deptno between 11 and 21  and deptno in (10,20,30)""").show(truncate=False)
      spark.sql("""select * from emp where deptno between 11 and 21  or deptno in (10,20,30)""").show(truncate=False)
class join:
    #on top of joined data we can use filter by using where condition .
    def self(self):
      spark.sql("""select e.empno emp_empo,e.ename emp_ename,e.sal emp_sal, m.empno mgr_empo,m.ename mgr_ename,m.sal mgr_sal from emp e right join emp m on e.empno = m.mgr""").show(truncate=False)
    
    def equi(self):
      spark.sql("""select * from emp inner join dept on emp.deptno = dept.deptno""").show(truncate=False)
    
    def cross(self):
      spark.sql("""select * from emp cross join dept where emp.deptno = dept.deptno""").show(truncate=False)
    
    def nonequi(self):

      spark.sql("""select * from emp inner join salgrade on emp.sal between lowsal and hisal""").show(truncate=False)
    
    def left(self):

      spark.sql("""select * from emp left join dept on emp.deptno = dept.deptno""").show(truncate=False)
    
    def anti(self):

      spark.sql("""select  * from dept anti join emp on emp.deptno = dept.deptno""").show(truncate=False)
    
    def right(self):

      spark.sql("""select * from emp right join dept on emp.deptno = dept.deptno""").show(truncate=False)
    
    def full(self):

      spark.sql("""select * from emp full outer join dept on emp.deptno = dept.deptno""").show(truncate=False)
class array:
    def contains(self):
      spark.sql("""select array_contains(Array("a","b","c"),"a") """).show(truncate=False)
    
    def distinct(self):
      spark.sql("""select array_distinct(Array("a","b","c")) """).show(truncate=False)

    
    def except_01(self):
      spark.sql("""select array_except(Array("a","b","c"),Array("c")) """).show(truncate=False)

    
    def intersect(self):
      spark.sql("""select array_intersect(Array("a","b","c"),Array("c")) """).show(truncate=False)

    
    def join(self):
      spark.sql("""select array_join(Array("a","b","c"),"","anil") """).show(truncate=False)

    
    def max_min(self):
      spark.sql("""select array_max(Array("a","b","c") ) """).show(truncate=False)

    
    def posission(self):
      spark.sql("""select array_position(Array("a","b","c"),"c") """).show(truncate=False)

    
    def remove(self):
      spark.sql("""select array_remove(Array("a","b","c"),"a") """).show(truncate=False)

    
    def sort(self):
      spark.sql("""select array_sort(Array("a","b","c")) """).show(truncate=False)

    
    def union(self):
      spark.sql("""select array_union(Array("a","b","c"),Array("c")) """).show(truncate=False)

    
    def zip(self):
      spark.sql("""select arrays_zip(Array("a","b","c"),Array("c")) """).show(truncate=False)

    
    def explode(self):
      spark.sql("""select  explode(Array("a","b","c")) """).show(truncate=False)

    
    def reverse(self):
      spark.sql("""select  reverse(Array("a","b","c")) """).show(truncate=False)

     #same command for array and non array
    def slice(self):
      spark.sql("""select slice(Array("a","b","c"),2,1) """).show(truncate=False)
class pivot_unpivot:
     
     def pivot(self):

       pivotSchema = StructType([StructField("Product",StringType(),True),
         StructField("Amount",IntegerType(),True),
         StructField("Country",StringType(),True) ])
       pivot = spark.read.option("delimiter","|").schema(pivotSchema).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\pivot.csv")
      #pivot.show(truncate=False)
       pivot.groupBy("Product").pivot("Country").sum("Amount").show(truncate=False)
       pivot.groupBy("Product").pivot("Country").sum("Amount""").show(truncate=False)
class loops:
   def case_when(self):
     spark.sql(""" select case when substr(ename,1,1) = "A" then "Statring with a" 
                        when substr(ename,1,1) = "M" then "Statring with a"
                        else "default" end as loop from  emp".stripMargin""").show(truncate=False)
print("Before main")
if __name__ == "__main__":
 order_of_execution()
 string().subStringByChar()
 print("In main")
print("After main")