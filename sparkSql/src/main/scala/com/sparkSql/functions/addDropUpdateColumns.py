import findspark
findspark.init()
from pyspark.sql.types import *
from pyspark.sql.functions import *
from bigData.pySpark import sparkSql


class add_drop_update_columns:

    salGradeSchema = StructType([StructField("GRADENUMBER",StringType(),True),
    StructField("LOWSAL",IntegerType(),True),
    StructField("HISAL",IntegerType(),True) ])

    #spark = SparkSession.builder.appName("read_and_write_hdfs").master("local").getOrCreate()
    salgradeDF = sparkSql.spark.read.option("delimiter", "|").schema(salGradeSchema).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\column_operations.txt")

    def addNewColumn(self):
      #data type update will not create new column
      sparkSql.salgradeDF.withColumn("GRADENUMBER", col("GRADENUMBER").cast("Integer")).show(truncate=False)
      sparkSql.salgradeDF.withColumn('GRADE', sparkSql.salgradeDF.GRADENUMBER.cast("Integer") + 1).show(truncate=False)
      sparkSql.salgradeDF.withColumn("a", lit("a")).show(truncate=False)

    def update_dataType(self):
      sparkSql.salgradeDF.withColumn("GRADENUMBER", col("Gradenumber").cast("Integer")).printSchema()
    def update_column_data(self):
     sparkSql.salgradeDF.withColumn("GRADENUMBER", sparkSql.salgradeDF["GRADENUMBER"].cast("integer") + 1).show(truncate=False)
    def update_column_name(self):
     sparkSql.salgradeDF.withColumnRenamed("gradenumber", "grade").show(truncate=False)
    def dropColumn(self):
     sparkSql.salgradeDF.drop("lowsal").show(truncate=False)
add_drop_update_columns().update_column_name()
  


  

