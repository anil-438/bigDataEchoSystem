package com.sparkSql.functions
import SourceDF._
object subQuery {
  empdf.createOrReplaceTempView("emp")
  deptdf.createOrReplaceTempView("dept")
  SalGradedf.createOrReplaceTempView("salGrade")

  def main(args: Array[String]): Unit = {
//Remove duplicate records from table.
    spark.sql("""select * from (select row_number() over(partition by sal order by sal) romNum,* from emp) where romNum = 1  """).show(false)
//Display duplicate records from the table.
    spark.sql("""select * from (select row_number() over(partition by sal order by sal) romNum,* from emp) where romNum <> 1  """).show(false)

    //Display the nth record from the table.
    spark.sql("""select * from (select row_number() over( order by sal) romNum,* from emp) where romNum = 5  """).show(false)

    //Display top 5 maximum salaries.
    spark.sql("""select * from (select row_number() over( order by sal desc) romNum,* from emp) where romNum <= 5  """).show(false)

    //Display only odd position records
    spark.sql("""select * from (select row_number() over( order by sal desc) romNum,* from emp) where mod(romNum,2) = 1  """).show(false)

    //Display only odd values.
    spark.sql("""select * from emp where mod(sal,2) = 0  """).show(false)

  }

}
