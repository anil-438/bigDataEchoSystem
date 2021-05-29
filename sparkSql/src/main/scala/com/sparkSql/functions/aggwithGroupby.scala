package com.sparkSql.functions
import aggFun._
import org.apache.spark.sql.types._
import stringFun._
object aggwithGroupby {
  df.createOrReplaceTempView("emp")
  def main(args: Array[String]): Unit = {
    spark.sql("""select deptno,Sum(sal),
                |avg(sal),
                |min(sal),
                |max(sal),
                |count(*),
                |collect_list(sal),
                |first(sal),
                |last(sal) from emp group by deptno""".stripMargin).show(false)

    spark.sql("""select deptno,Sum(sal),
                |avg(sal),
                |min(sal),
                |max(sal),
                |count(*),
                |collect_list(sal),
                |first(sal),
                |last(sal) from emp group by rollup(deptno)""".stripMargin).show(false)


  }
}
