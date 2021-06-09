import findspark
findspark.init()
from pyspark.sql.functions import *
from bigData.pySpark.Schema import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql import Row

spark = SparkSession
.builder
.appName("transforms_and_actions")
.master("local")
.getOrCreate()

sc = spark.sparkContext


class transforms_and_actions :

  def txt(self):
    txt_file = sc.textFile("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt")
    # Method 01 --> createDataFrame with structType
    txt_RDD = txt_file.map(lambda x: x.split(",")).map(lambda col: Row(col[0], int(col[1])))
    spark.createDataFrame(txt_RDD, Schema().structtype()).show(truncate=False)
    # Method 02 --> case class with toDF
    txt_RDD_02 = txt_file.map(lambda x: x.split(",")).map(
      lambda col: Row(name=col[0], age=int(col[1]))).toDF()  # .trim.toInt --> Madatory for integer columns
    # Method 03 --> seq with toDF
    spark.read.option("delimiter", ",").csv(
      "C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").toDF(*["name", "age"]).show(
      truncate=False)
    # Method 04 --> hard Coded with toDF
    spark.read.option("delimiter", ",").csv(
      "C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").toDF("name", "age").show(truncate=False)
    # Method 05 --> schema with struct typ
    spark.read.option("delimiter", ",").schema(Schema().structtype().add("_corrupt_record", StringType(), True)).csv(
      "C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").show(truncate=False)
    txt_RDD_02.show(truncate=False)

    #source.empDF.filter(!col("comm").isNull).show(false)
    # df.select(col("a")).filter(col("s").like("a%"))
  def source(self):

    sc.setLogLevel("off")
    rdd  =sc.textFile("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\emp.txt")
    df = spark.read \
                  .format("csv") \
                  .option("header","true") \
                  .option("delimiter","|") \
                  .schema(StructType([StructField("EMPNO",IntegerType(),True),
                                      StructField("ENAME",StringType(),True),
                                      StructField("JOB",StringType(),True),
                                      StructField("MGR",IntegerType(),True),
                                      StructField("HIREDATE",StringType(),True),
                                      StructField("SAL",IntegerType(),True),
                                      StructField("COMM",IntegerType(),True),
                                      StructField("DEPTNO",IntegerType(),True)])) \
                  .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\emp.txt")
    return (rdd,df)
  class rdd_trasform:
    def __init__(self):
      self.rdd = transforms_and_actions().source()[0]

    def map(self):
      self.rdd.map(lambda  c: c.split("|")).map(lambda col : Row(col[0],int(col[1])))
    def filter(self):
       print(self.rdd.map(lambda c: c.split("|")).filter(lambda col: col[0] == "7369").collect())
    def collect(self):
         print(self.rdd.map(lambda c: c.split("|")).collect())
    def saveastextfile(self):
        self.rdd.saveAsTextFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\saveAsTextFile.txt")
    def subtract(self):
        self.rdd.subtract(self.rdd)
    def count(self):
        self.rdd.count()
    def intersect(self):
       self.rdd.intersection(self.rdd)
    def cache(self):
       self.rdd.cache()
    def persist(self):
      self.rdd.persist()
    def coalesce (self):
      self.rdd.coalesce(3).saveAsTextFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\coalesce.txt")
    def repartition(self):
      self.rdd.repartition(3).saveAsTextFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\coalesce.txt")
    def distinct(self):
      self.rdd.distinct()
    def first(self):
      print(self.rdd.first())
    def take(self):
      print(self.rdd.take(5))
    def isempty(self):
      print(self.rdd.isEmpty())
    def union(self):
      print(self.rdd.union(self.rdd).collect())
  class df_trasform:
    def __init__(self):
      self.df = transforms_and_actions().source()[1]
    def tranc(self):
      self.df.union(self.df)
      self.df.take(4)
      self.df.first()
      self.df.distinct()
      self.df.repartition(5)
      self.df.coalesce(5)
      self.df.cache()
      self.df.persist()
      self.df.count()
      self.df.subtract(self.df)
      self.df.filter(col("ENAME").isNull())
      self.df.drop("ENAME")
      self.df.show(truncate=False)
      self.df.select("ENAME")
      self.df.join(self.df,self.df['ename'] == self.df['ename'],"inner")
      self.df.createOrReplaceTempView("emp")
      print(self.df.columns)

transforms_and_actions().df_trasform().tranc()



