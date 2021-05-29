package com.sparkSql.functions
import stringFun._
object loops {

  def main(args: Array[String]): Unit = {
    spark.sql(
      """select case
        |when "ename" == "anil" then "my name is anil"
        |when "anil" <> "anil" then "my name is not anil"
        |  else "known name" end ,
        |
        |  if("ename" = "anil",1,0)""".stripMargin).show(false)

  }
}
