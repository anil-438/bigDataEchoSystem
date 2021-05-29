package com.sparkSql.functions
import com.sparkSql.functions.SourceDF._
object orderBy {
empdf.createOrReplaceTempView("emp")

  def main(args: Array[String]): Unit = {
    spark.sql("""select sal,comm from emp order by comm asc""")show(false)
    spark.sql("""select sal,comm from emp order by comm desc""")show(false)
  }
}
