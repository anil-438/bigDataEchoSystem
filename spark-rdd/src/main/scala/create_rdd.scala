import org.apache.spark.sql.SparkSession
//we can create
object create_rdd {

  implicit val spark = SparkSession.builder().appName("read_and_write_hdfs").master("local").getOrCreate()
// from txt file
 // sc.textFile will support for txt and csv file
  val rdd = spark.sparkContext.textFile("C:/tmp/files/*") // all files in dir
  val rddWhole = spark.sparkContext.wholeTextFiles("C:/tmp/files/*")  //wholeTextFiles
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt") // multiple files
  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/text*.txt") // matching to patren
  val rdd4 = spark.sparkContext.textFile("C:/tmp/dir1/*,C:/tmp/dir2/*,c:/tmp/files/text01.txt") //multiple dirs and files

    // from data frame
    val myRdd2 = spark.read.text("C:/tmp/files/*").toDF().rdd
  val myRdd = spark.read.text("C:/tmp/files/*").rdd




}
