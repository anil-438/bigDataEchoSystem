package com.sparkSql.functions
import  SourceDF._
import org.apache.spark.sql.types._
object scd {
  empdf.createOrReplaceTempView("emp")
  deptdf.createOrReplaceTempView("dept")
  SalGradedf.createOrReplaceTempView("salGrade")
  val empSchema = StructType(Seq(StructField("EMPNO",IntegerType,true),
    StructField("ENAME",StringType,true),
    StructField("JOB",StringType,true),
    StructField("MGR",IntegerType,true),
    StructField("HIREDATE",StringType,true),
    StructField("SAL",IntegerType,true),
    StructField("COMM",IntegerType,true),
    StructField("DEPTNO",IntegerType,true),
    StructField("surrogate_key",IntegerType,true)))
  val history = spark.read.option("delimiter","|").schema(empSchema).csv("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\history_data.txt")
  val delta=spark.read.option("delimiter","|").schema(empSchema).csv("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\deltaFile.txt").drop("surrogate_key")
  history.createOrReplaceTempView("history")
  delta.createOrReplaceTempView("delta")

  def surrogateKey: String = {
    //var uniqueId = history.agg(max("No"))\
    import spark.implicits._
    var uniqueId = spark.sql(s"""SELECT MAX (surrogate_key) FROM history""").rdd

    uniqueId.collect.map(c => c(0)).mkString

  }
  def scd1: Unit ={
    val delta_01 = spark.sql(s"""select *, cast($surrogateKey as integer)+row_number() over(order by delta.EMPNO) as surrogate_key from delta """)
    delta_01.show()
    delta_01.createOrReplaceTempView("delta_01")
    val updatedRecords= spark.sql(s"""select delta_01.EMPNO,delta_01.ENAME,delta_01.JOB,delta_01.MGR, delta_01.HIREDATE,delta_01.SAL,delta_01.COMM,delta_01.DEPTNO, delta_01.surrogate_key from history inner join delta_01 on history.empno = delta_01.empno""")

    val newRecords = spark.sql(s"""select delta_01.EMPNO,delta_01.ENAME,delta_01.JOB,delta_01.MGR, delta_01.HIREDATE,delta_01.SAL,delta_01.COMM,delta_01.DEPTNO, delta_01.surrogate_key from delta_01 anti join history on history.empno = delta_01.empno""")

    val notModified =spark.sql(s"""select history.* from history anti join delta_01 on history.empno = delta_01.empno""")

    notModified.union(newRecords).union(updatedRecords).distinct().sort("surrogate_key").show(false)
  }
  def scd2: Unit ={
    //max(histrory.surrogateKey)+row_number
    //max(histrory.surrogateKey)+accumilator
  }
  def scd3{}
  def main(args: Array[String]): Unit = {
scd1
  }
}
