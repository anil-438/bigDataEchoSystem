import findspark
findspark.init()
from bigData.pySpark.Schema import *
#Text data source does not support array<int> data type
#json write not support snappy compression
"""   .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")

"""

spark = SparkSession.builder.appName("read_and_write_hdfs").master("local").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("off")

class read_and_write_hdfs:
 
  def txt(self):

    txt_file = sc.textFile("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt")
    #Method 01 --> createDataFrame with structType
    txt_RDD=txt_file.map(lambda x: x.split(",")).map( lambda col : Row(col[0],int(col[1])))
    spark.createDataFrame(txt_RDD,Schema().structtype()).show(truncate=False)
    #Method 02 --> case class with toDF
    txt_RDD_02=txt_file.map(lambda x: x.split(",")).map(lambda col : Row(name=col[0],age=int(col[1]))).toDF() #.trim.toInt --> Madatory for integer columns
    #Method 03 --> seq with toDF
    spark.read.option("delimiter", ",").csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").toDF(*["name", "age"]).show(truncate=False)
    #Method 04 --> hard Coded with toDF
    spark.read.option("delimiter", ",").csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").toDF("name","age").show(truncate=False)
    #Method 05 --> schema with struct typ
    spark.read.option("delimiter", ",").schema(Schema().structtype().add("_corrupt_record",StringType(),True)).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").show(truncate=False)
    txt_RDD_02.show(truncate=False)
  
  def parquet(self):
    parquet = spark.read\
       .format("parquet")\
       .option("header","true")\
       .option("mode", "DROPMALFORMED")\
       .option("mergeSchema", "true")\
       .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.parquet")

    parquet.repartition(1) \
       .write \
      .partitionBy("favorite_color") \
      .format("parquet") \
      .mode('overwrite') \
      .option("compression", "snappy") \
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\namesPartByColor.parquet")

  def avro(self):
       avro = spark.read \
      .format("avro") \
      .option("header","true") \
      .option("mode", "DROPMALFORMED") \
      .option("mergeSchema", "true") \
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\users.avro")

       avro.repartition(1)  \
      .write \
      .partitionBy("favorite_color") \
      .format("avro") \
      .mode('Overwrite') \
      .option("compression", "snappy") \
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\namesPartByColor.avro")

  def json(self):

    json = spark.read \
      .format("json") \
      .option("header","true") \
      .option("mode", "DROPMALFORMED") \
      .option("mergeSchema", "true") \
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.json")
    json.show(truncate=False)
    json.repartition(1) \
      .write \
      .partitionBy("name") \
      .format("json") \
      .mode('overwrite') \
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\name.json")

  def csv(self):
    peopleDFCsv = spark.read.format("csv") \
      .option("sep", ";") \
      .option("inferSchema", "true") \
      .option("header", "true") \
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.csv")

    csv = spark.read \
      .format("csv") \
      .option("header","true") \
        .option("delimiter", "|") \
        .option("mode", "DROPMALFORMED") \
      .option("mergeSchema", "true") \
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.csv")

    csv.repartition(1) \
      .write \
      .option("header","true") \
      .format("csv") \
      .mode('overwrite') \
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\name.csv")
    csv.show(truncate=False)

  def orc(self,A="A"):
    orc = spark.read \
      .format("orc") \
      .option("header","true") \
      .option("mode", "DROPMALFORMED") \
      .option("mergeSchema", "true") \
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.orc")

    orc.repartition(1) \
      .write \
      .format("orc") \
      .mode('overwrite') \
      .option("compression", "snappy") \
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\namesPartByColor.orc")
    orc.show(truncate=False)
read_and_write_hdfs().orc()

  



