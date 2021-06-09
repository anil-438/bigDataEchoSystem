package spark.dataFrame

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._


//Text data source does not support array<int> data type
//json write not support snappy compression
/*    .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")

 */
object read_and_write_hdfs {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Winutils\\")
   implicit val spark = SparkSession.builder().appName("read_and_write_hdfs").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("off")
     /* txt(spark,sc)
     parquet(spark)
     avro(spark)
     json(spark)
      csv(spark)
    orc(spark)
     */
    val txt_file = sc.textFile("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt")
    txt_file.foreach(println)
  }
  def txt(spark:SparkSession,sc:SparkContext): Unit ={

    val txt_file = sc.textFile("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt")
    // Method 01 --> createDataFrame with structType
    val txt_RDD=txt_file.map(_.split(",")).map(col => Row(col(0),col(1)))
    spark.createDataFrame(txt_RDD,Schema.structType ).show(false)
    // Method 02 --> case class with toDF
   // val txt_RDD_02=txt_file.map(_.split(",")).map(col => Schema.Person(col(0),col(1).trim.toInt)).toDF() //.trim.toInt --> Madatory for integer columns
    //Method 03 --> seq with toDF
    spark.read.option("delimiter", ",").csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").toDF(Schema.seq:_*).show(false)
    //Method 04 --> hard Coded with toDF
    spark.read.option("delimiter", ",").csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").toDF("name","age").show(false)
    //Method 05 --> schema with struct type
    spark.read.option("delimiter", ",").schema(Schema.structType.add("_corrupt_record",StringType,true)).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt").show(false)

  }
  def parquet(spark:SparkSession): Unit ={
val parquet = spark.read
  .format("parquet")
  .option("header","true")
  .option("mode", "DROPMALFORMED")
  .option("mergeSchema", "true")
  .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.parquet")

    parquet.repartition(1)
      .write
      .partitionBy("favorite_color")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\namesPartByColor.parquet")

  }
  def avro(spark:SparkSession): Unit ={
    val avro = spark.read
      .format("avro")
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .option("mergeSchema", "true")
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\users.avro")

    avro.repartition(1)
      .write
      .partitionBy("favorite_color")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\namesPartByColor.avro")

  }
  def json(spark:SparkSession): Unit ={
    val json = spark.read
      .format("json")
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .option("mergeSchema", "true")
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.json")
json.show(false)
    json.repartition(1)
      .write
      .partitionBy("name")
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\name.json")

  }
  def csv(spark:SparkSession): Unit ={
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.csv")

    val csv = spark.read
      .format("csv")
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .option("mergeSchema", "true")
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.csv")

    csv.repartition(1)
      .write
      .option("header","true")
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\name.csv")


  }
  def orc(spark:SparkSession): Unit ={
    val orc = spark.read
      .format("orc")
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .option("mergeSchema", "true")
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.orc")

    orc.repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\test\\namesPartByColor.orc")

  }


}
