package com.sparkSql.functions
import  stringFun._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object dateFun {

  def main(args: Array[String]): Unit = {
    import spark.implicits
    spark.sql("""select
                |to_date("20210510","yyyyMMdd"),
                |current_date(),
                |date_add(to_date("20210510","yyyyMMdd"),5),
                |date_sub(to_date("20210510","yyyyMMdd"),5),
                |dayofmonth(to_date("20210510","yyyyMMdd")),
                |dayofweek(to_date("20210510","yyyyMMdd")),
                |dayofyear(to_date("20210510","yyyyMMdd")),
                |datediff(to_date("20210510","yyyyMMdd"),to_date("20210610","yyyyMMdd"))
                """.stripMargin).show(false)
  }


}
