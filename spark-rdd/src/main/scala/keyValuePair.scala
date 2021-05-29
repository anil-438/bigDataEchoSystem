import org.apache.spark.sql.SparkSession

object keyValuePair {


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
    var data = sc.parallelize(Array(("C",3),("A",1),("B",4),("A",2),("B",5)))
    val datasetrdd = rdd.map(x => (x,1))
    //data=datasetrdd
    val reducefunc = data.reduceByKey((value, x) => (value + x))
    println(reducefunc.collect.mkString)
    val aggfunc = data.aggregateByKey(0)((x, y)=> x+y, (k, v)=> k+v )
    println(aggfunc.collect.mkString)
    val groupfun = data.groupByKey
    println(groupfun.collect.mkString)
    val reducefun = data.reduceByKey((a,b)=> a+b)
    println(reducefun.collect.mkString)
    val countbykey = data.countByKey()
    val countbyvalue = data.countByValue()
    println(countbykey)
    println(countbyvalue)
    val Cogroupfun = data.cogroup(datasetrdd)
    println(Cogroupfun.collect().mkString)
  }



}
