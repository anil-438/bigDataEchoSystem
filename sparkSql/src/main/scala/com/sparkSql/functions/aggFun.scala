package com.sparkSql.functions
import org.apache.spark.sql.types._
import stringFun._

object aggFun {
  val empSchema = StructType(Seq(StructField("EMPNO",IntegerType,true),
    StructField("ename",StringType,true),
    StructField("JOB",StringType,true),
    StructField("MGR",IntegerType,true),
    StructField("HIREDATE",StringType,true),
    StructField("SAL",IntegerType,true),
    StructField("COMM",IntegerType,true),
    StructField("DEPTNO",IntegerType,true) ))
  import spark.implicits._
  val df = spark.read
    .schema(empSchema)
    .format("csv")
    .option("delimiter","|")
    .load("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\emp.txt")
  df.createOrReplaceTempView("emp")
  def main(args: Array[String]): Unit = {
    spark.sql("""select Sum(sal),
                |avg(sal),
                |min(sal),
                |max(sal),
                |count(*),
                |collect_list(sal),
                |first(sal),
                |last(sal) from emp """.stripMargin).show(false)
  }

}
