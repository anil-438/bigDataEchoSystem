package spark.dataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object createDataFrame {
  System.setProperty("hadoop.home.dir", "C:\\Winutils\\")
val spark =SparkSession
             .builder()
             .master("local")
             .appName("df")
             .getOrCreate()
spark.sparkContext.setLogLevel("off")
  val empSchema = StructType(Seq(StructField("EMPNO",IntegerType,true),
    StructField("ENAME",StringType,true),
    StructField("JOB",StringType,true),
    StructField("MGR",IntegerType,true),
    StructField("HIREDATE",StringType,true),
    StructField("SAL",IntegerType,true),
    StructField("COMM",IntegerType,true),
    StructField("DEPTNO",IntegerType,true) ))
  val df = spark.read
    .format("csv")
    .option("delimiter","|")
    .option("header","false")
    .schema(empSchema)
    .load("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\emp.txt")

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    //columns
    println(df.columns.map(c => "New_"+df(c)).mkString("|"))
    //select clause operetions
    df.select(col("job"),df("ename"),$"sal").show(false)
    df.select("sal").show(false)
    df.select($"sal".substr(1,2)).show(false)
    //where clause
    df.filter(df("sal").like("10%")).show(false)
//join clause
    val df1 = df
    df.join(broadcast(df1),df("ename")===df1("ename")).show(false)
    df.join(df1,df("ename")===df1("ename"),"inner").show(false)
//union
    df.union(df1).show(false)

//group by
    println("group by")
    df.groupBy("deptno").sum("sal").show(false)

//Sort clause
    df.sort("deptno").show(false)

// not in sql
    df.agg((sum("sal"))).show(false)
    df.withColumn("sal",$"sal"+100).show(false)
    df.distinct().show(false)
    df.drop("sal").show(false)
    df.withColumnRenamed("sal","salary").show(false)
  }

}
