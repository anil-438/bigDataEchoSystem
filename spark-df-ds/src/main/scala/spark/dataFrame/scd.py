import findspark
findspark.init()
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("read_and_write_hdfs").master("local").getOrCreate()
sc = spark.sparkContext
empSchema = StructType([StructField("EMPNO", IntegerType(), True),
                        StructField("ENAME", StringType(), True),
                        StructField("JOB", StringType(), True),
                        StructField("MGR", IntegerType(), True),
                        StructField("HIREDATE", StringType(), True),
                        StructField("SAL", IntegerType(), True),
                        StructField("COMM", IntegerType(), True),
                        StructField("DEPTNO", IntegerType(), True),
                        StructField("surrogate_key", IntegerType(), True)])
history = spark.read.option("delimiter", "|").schema(empSchema).csv("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\history_data.txt")
delta = spark.read.option("delimiter", "|").schema(empSchema).csv("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\deltaFile.txt").drop("surrogate_key")
history.createOrReplaceTempView("history")
delta.createOrReplaceTempView("delta")

class scd :

  def surrogateKey(self):
    #var uniqueId = history.agg(max("No"))\
    uniqueId = spark.sql("""SELECT MAX (surrogate_key) FROM history""").rdd
    #uniqueId = history.select(max(col("surrogate_key"))).rdd.collect()
    return str(uniqueId.collect())[str(uniqueId.collect()).index("=")+1:len(str(uniqueId.collect()))-2]

  def scd1(self):
    sc.setLogLevel("off")
    delta_01 = spark.sql("select *, cast({max_surr} as integer)+row_number() over(order by delta.EMPNO) as surrogate_key from delta ".format(max_surr = int(scd().surrogateKey())))
    delta_01.show()
    delta_01.createOrReplaceTempView("delta_01")
    updatedRecords= spark.sql("""select delta_01.EMPNO,delta_01.ENAME,delta_01.JOB,delta_01.MGR, delta_01.HIREDATE,delta_01.SAL,delta_01.COMM,delta_01.DEPTNO, delta_01.surrogate_key from history inner join delta_01 on history.empno = delta_01.empno""")
    newRecords = spark.sql("""select delta_01.EMPNO,delta_01.ENAME,delta_01.JOB,delta_01.MGR, delta_01.HIREDATE,delta_01.SAL,delta_01.COMM,delta_01.DEPTNO, delta_01.surrogate_key from delta_01 anti join history on history.empno = delta_01.empno""")
    nonModified =spark.sql("""select history.* from history anti join delta_01 on history.empno = delta_01.empno""")
    nonModified.union(newRecords).union(updatedRecords).distinct().sort("surrogate_key").show(truncate=False)
    
  def scd2(self):
    #max(histrory.surrogateKey)+row_number
    #max(histrory.surrogateKey)+accumilator
    print("hi")
  
  def scd3(self):
    print("hi")
 
scd().scd1()

