package com.sparkSql.functions
import stringFun._
object numericFun {

  def main(args: Array[String]): Unit = {
    spark.sql("""select Abs(123.12),
                |ceiling(123.12),
                |floor(123.12),
                |round(123.12),
                |format_number(123.12,"##.#"),
                |mod(4,2),
                |4+2 ,
                |4-2,
                |4/2,
                |4*2,
                |power(4,2),
                |cbrt(4),
                |sqrt(9)""".stripMargin).show(false)
  }
}
