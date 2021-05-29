package com.sparkSql.functions
import stringFun._
object typeCast {
  def main(args: Array[String]): Unit = {
    spark.sql("""select
                |date_format('2016-04-08', 'yyyy-MM'),
                |cast("100" as Int) ,
                |cast(100 as decimal(23,2)),
                | cast(100.00 as string)""".stripMargin).show(false)
  }

}
