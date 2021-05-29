package com.sparkSql.functions
import aggFun._
import stringFun._
object windowsFun {

  def first_last_Value: Unit ={
    spark.sql("""select deptno,sal, first_value(sal) over (partition by deptno order by sal)from emp""").show(false)
    spark.sql("""select deptno,sal, first_value(sal) over(partition by deptno) from emp""").show(false)
    spark.sql("""select deptno,sal, first_value(sal) over() from emp""").show(false) // 1 -1 output
    spark.sql("""select  first_value(sal) from emp""").show(false)  // out put one record
  }

  def lag_lead_Fun: Unit ={
    spark.sql("""select deptno,sal, sal-lag(sal,1,null) over(partition by deptno order by sal) from emp""").show(false)
    spark.sql("""select deptno,sal, sal-lag(sal,1) over(partition by deptno order by sal) from emp""").show(false)
    spark.sql("""select deptno,sal, sal-lag(sal) over(partition by deptno order by sal) from emp""").show(false)
    spark.sql("""select deptno,sal, sal-lag(sal,1,null) over(order by deptno) from emp""").show(false)
  //  spark.sql("""select deptno,sal, sal-lag(sal,1,null) over() from emp""").show(false)
   // spark.sql("""select deptno,sal, sal-lag(sal,1,null)  from emp""").show(false)

  }
  def Rank_DENSE_PERCENT: Unit ={
    spark.sql("""select sal,deptno ,rank(sal) over(partition by deptno order by sal) from emp""").show(false)
    spark.sql("""select sal,deptno ,DENSE_RANK(sal) over(partition by deptno order by sal) from emp""").show(false)
    spark.sql("""select sal,deptno ,PERCENT_RANK(sal) over(partition by deptno order by sal) from emp""").show(false)
  }
  def CUME_DIST: Unit ={
    spark.sql("""select sal,deptno ,CUME_DIST() over(partition by deptno order by sal) from emp""").show(false)
    spark.sql("""select sal,deptno ,CUME_DIST() over(order by sal) from emp""").show(false)

  }
  def ntil: Unit ={
    spark.sql(s"""select empno,deptno,sal,ntile(3) over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,ntile(3) over(order by sal) from emp""" ).show(false)

  }

  def row_num: Unit ={
    spark.sql(s"""select empno,deptno,sal,row_number() over(partition by deptno order by sal) from emp order by deptno,sal""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,row_number() over(order by sal) from emp""" ).show(false)

  }
def sum: Unit ={
  spark.sql(s"""select empno,deptno,sal,sum(sal) over(partition by deptno order by sal) from emp""" ).show(false)
  spark.sql(s"""select empno,deptno,sal,sum(sal) over(partition by deptno) from emp""" ).show(false)
  spark.sql(s"""select empno,deptno,sal,sum(sal) over() from emp""" ).show(false)
  spark.sql(s"""select sum(sal) from emp""" ).show(false)
}
  def avg: Unit ={
    spark.sql(s"""select empno,deptno,sal,avg(sal) over(partition by deptno order by sal) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,avg(sal) over(partition by deptno) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,avg(sal) over() from emp""" ).show(false)
    spark.sql(s"""select avg(sal) from emp""" ).show(false)
  }
  def min_max: Unit ={
    spark.sql(s"""select empno,deptno,sal,min(sal) over(partition by deptno order by sal) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,min(sal) over(partition by deptno) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,min(sal) over() from emp""" ).show(false)
    spark.sql(s"""select min(sal) from emp""" ).show(false)
  }
  def count: Unit ={
    spark.sql(s"""select empno,deptno,sal,count(sal) over(partition by deptno order by sal) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,count(sal) over(partition by deptno) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,count(sal) over() from emp""" ).show(false)
    spark.sql(s"""select count(sal) from emp""" ).show(false)

    spark.sql(s"""select empno,deptno,sal,count(*) over(partition by deptno order by sal) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,count(*) over(partition by deptno) from emp""" ).show(false)
    spark.sql(s"""select empno,deptno,sal,count(*) over() from emp""" ).show(false)
    spark.sql(s"""select count(*) from emp""" ).show(false)
  }
  def main(args: Array[String]): Unit = {
    df.createOrReplaceTempView("emp")
    min_max
  }

}
