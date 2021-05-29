package com.sparkSql.functions
import SourceDF._
object joins {
empdf.createOrReplaceTempView("emp")
deptdf.createOrReplaceTempView("dept")
 SalGradedf.createOrReplaceTempView("SalGrade")

  def main(args: Array[String]): Unit = {
    spark.sql(
      """select emp.*,dept.* from emp,dept
                                     where emp.deptno = dept.deptno""").show(false)

    spark.sql(
      """select emp.*,dept.*
                 from emp inner join dept
                 on emp.deptno = dept.deptno""").show(false)

    spark.sql(
      """select emp.*,dept.*
                 from emp anti join dept
                 on emp.deptno = dept.deptno""").show(false)
    spark.sql(
      """select emp.*,SalGrade.*
                 from emp inner join SalGrade
                 on emp.sal between SalGrade.lowsal and SalGrade.hisal""").show(false)
    spark.sql(
      """select emp.*,SalGrade.*,dept.*
                 from emp
                 inner join SalGrade
                 on emp.sal between SalGrade.lowsal and SalGrade.hisal
                 inner join dept
                 on emp.deptno = dept.deptno """).show(false)

    spark.sql(
      """select emp.*,dept.*
                 from emp right join dept
                 on emp.deptno = dept.deptno""").show(false)
    spark.sql(
      """select emp.*,dept.*
                 from emp left join dept
                 on emp.deptno = dept.deptno""").show(false)
    spark.sql(
      """select emp.*,dept.*
                 from emp full outer join dept
                 on emp.deptno = dept.deptno""").show(false)

  }

}
