package com.sparkSql.functions
import SourceDF._
object where {
empdf.createOrReplaceTempView("emp")
SalGradedf.createOrReplaceTempView("SaleGrade")

  def main(args: Array[String]): Unit = {
    spark.sql("""select *,comm as com from emp where comm = 500""").show(false)
    spark.sql("""select * from emp where comm == 500""").show(false)
    spark.sql("""select * from emp where comm != 500""").show(false)
    spark.sql("""select * from emp where comm <> 500""").show(false)
    spark.sql("""select * from emp where comm < 500""").show(false)
    spark.sql("""select * from emp where comm > 500""").show(false)
    spark.sql("""select * from emp where comm <= 500""").show(false)
    spark.sql("""select * from emp where comm >= 500""").show(false)
    spark.sql("""select * from emp where isNull(comm) """).show(false)
    spark.sql("""select * from emp where isnotnull(comm)  """).show(false)
    spark.sql("""select * from emp where comm in (300,500,1000) """).show(false)
    spark.sql("""select * from emp where comm not in (300,500,1000)  """).show(false)
    spark.sql("""select ename,length(ename) from emp where length(ename) > 4""").show(false)
    spark.sql("""select * from emp where ename like "KI%"  """).show(false)
    spark.sql("""select * from emp where ename like "KI__"  """).show(false)
    spark.sql("""select * from emp where ename not like "KI%"  """).show(false)
    spark.sql("""select * from emp where ename not like "KI__"  """).show(false)
    spark.sql("""select * from emp where ename rlike "(KI|CL)"  """).show(false)
    spark.sql("""select * from emp where ename not rlike "(KI|CL)"  """).show(false)
  }
}
