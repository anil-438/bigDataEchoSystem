import org.apache.spark.sql.SparkSession

object createRDD {

  def main(args: Array[String]): Unit = {
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
        rdd.saveAsTextFile("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\rdd.txt")


  }
}
