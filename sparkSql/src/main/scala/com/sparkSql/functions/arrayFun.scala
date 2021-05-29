package com.sparkSql.functions
import stringFun._
import aggFun._
object arrayFun {
  def main(args: Array[String]): Unit = {
    df.createOrReplaceTempView("emp")
    spark.sql(
      """select
        |collect_list(sal),
        |array_contains(collect_list(sal),800),
        |array_distinct(collect_list(sal)),
        |array_except(collect_list(sal),collect_list(sal)),
        |array_intersect(collect_list(sal),collect_list(comm)),
        |array_join(collect_list(sal),"|"),
        |array_position(collect_list(sal),5000),
        |array_remove(collect_list(sal),800),
        |array_repeat(collect_list(sal),2),
        |array_sort(collect_list(sal)),
        |array_union(collect_list(sal),collect_list(sal)),
        |arrays_overlap(collect_list(sal),collect_list(sal)),
        |array_min(collect_list(sal)),
        |array_max(collect_list(sal)) from emp""".stripMargin).show(false)

spark.sql("""select explode(array_sal) from (select collect_list(sal) as array_sal from emp)""").show(false)
  }
}