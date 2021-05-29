import org.apache.spark.sql.SparkSession

object transformFunctions {
  val spark = SparkSession
    .builder()
    .appName("allContexts")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("off")
  val emptyrdd = sc.emptyRDD
  val rdd = sc.textFile("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\dept.txt")

  def main(args: Array[String]): Unit = {
    val rddFilter=rdd.filter(x => x.startsWith("20"))
    val rddMap=rdd.map(_.split('|')).filter(x => x(0)==("20")).map(x =>x(1))
    rddFilter.subtract(rddMap).foreach(println)
    rdd.intersection(rdd).foreach(println)
    println(rdd.count())
    (rddFilter++rddMap).foreach(println)
    rdd.cache().take(1).foreach(println)
    println(rdd.cache().first)



  }

}
