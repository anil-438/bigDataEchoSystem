import org.apache.spark.sql.SparkSession

object allContexts {
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//Acess spark rdd,dataframe, dataset and spark sql
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  val spark = SparkSession
    .builder()
    .appName("allContexts")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()
  val sc = spark.sparkContext
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//Access hadoop files
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  import org.apache.hadoop.fs._
  System.setProperty("hadoop.home.dir", "C:\\Winutils\\")
  val Hadoop_configuration  =sc.hadoopConfiguration
  val fs = FileSystem.get(Hadoop_configuration)


//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Acees hive db
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  val Hive_context=new org.apache.spark.sql.hive.HiveContext(sc)
  // Symbol HiveContext is deprecated. Use SparkSession.builder.enableHiveSupport instead
  // Procrss 1
  System.setProperty("spark.sql.warehouse.dir", "C:\\Hive\\apache-hive-2.3.7-bin\\")
  System.setProperty("hadoop.home.dir", "C:\\Winutils\\")
  val hivespark1 = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()
  //process 2
  import java.io.File
  val warehouseLocation=new File("C:\\Hive\\apache-hive-2.3.7-bin\\").getAbsolutePath
  val hivespark2 = SparkSession
    .builder()
    .appName("Spark Hive Example2")
    .master("local")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()


//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Acees Spark stream
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  import org.apache.spark._
  import org.apache.spark.streaming._

  val conf = new SparkConf().setAppName("appName").setMaster("local")
  val StreamContext = new StreamingContext(conf, Seconds(1))

// existing SparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
}
