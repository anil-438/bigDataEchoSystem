package com.sparkSql.functions

import com.sparkSql.functions.SourceDF._

object having {
empdf.createOrReplaceTempView("emp")
  def main(args: Array[String]): Unit = {
    spark.sql(
      """select deptno,max(sal) as maxSal
        |from emp
        |group by deptno
        |having maxSal > 2000 """.stripMargin).show(false)

  }

}
