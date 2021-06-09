import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

#parquet,avro,json,csv and orc file will have infer schema --> .option("header","true")

spark = SparkSession.builder.master("local").appName("anil").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("off")
class Schema :

  def structtype(self):

    schemaString = "name age"
    #fields = map(lambda fieldName:  StructField(fieldName, StringType(),True), schemaString.split(" "))
    fields =  [StructField(fieldName, StringType(),True) for fieldName in schemaString.split(" ")]
    return StructType(fields) #StructType(StructField(name,StringType,true), StructField(age,StringType,true))

  def hardCode(sels):

    lines = sc.textFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\people.txt")
    parts = lines.map(lambda l: l.split(","))
    schemaPeople = spark.createDataFrame(parts).toDF("name", "age")
    schemaPeople.show(truncate=False)

  def coll_creatdf (self):
     #coll = ["name", "age"]
     lines = sc.textFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\people.txt")
     parts = lines.map(lambda l: l.split(","))
     schemaPeople = spark.createDataFrame(parts,["name", "age"])
     schemaPeople.show(truncate=False)

  def coll_todf(sels):
    coll = ["name", "age"]
    lines = sc.textFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\people.txt")
    parts = lines.map(lambda l: l.split(","))
    schemaPeople = spark.createDataFrame(parts).toDF(*coll)
    #schemaPeople = spark.createDataFrame(parts).toDF(*["name", "age"])
    schemaPeople.show(truncate=False)

  def schems_creatdf (self):
     #coll = ["name", "age"]
     lines = sc.textFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\people.txt")
     parts = lines.map(lambda l: l.split(","))
     schemaPeople = spark.createDataFrame(parts, Schema().structtype())
     schemaPeople.show()

  def map (self):

    lines = sc.textFile("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.show(truncate=False)

  def schema(self):
   df = spark.read\
    .option("delimiter",",")\
    .schema(Schema().structtype().add("_corrupt_record",StringType(),True))\
    .csv("C:\\Users\\me\\PycharmProjects\\PyThon\\src\\main\\resource\\people.txt")
   df.show(truncate=False)


struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)

struct2 = StructType([StructField("f1", StringType(), True),
                      StructField("f2", StringType(), True, None)])


