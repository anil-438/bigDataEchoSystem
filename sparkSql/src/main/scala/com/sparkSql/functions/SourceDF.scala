package com.sparkSql.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SourceDF {
  val spark = SparkSession
    .builder()
    .appName("sql")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("off")
  val deptSchema = StructType(Seq(StructField("DEPTNO",IntegerType,true),
    StructField("DNAME",StringType,true),
    StructField("LOC",StringType,true) ))
  val empSchema = StructType(Seq(StructField("EMPNO",IntegerType,true),
    StructField("ename",StringType,true),
    StructField("JOB",StringType,true),
    StructField("MGR",IntegerType,true),
    StructField("HIREDATE",StringType,true),
    StructField("SAL",IntegerType,true),
    StructField("COMM",IntegerType,true),
    StructField("DEPTNO",IntegerType,true) ))
  val salGradeSchema = StructType(Seq(StructField("GRADENUMBER",IntegerType,true),
    StructField("LOWSAL",IntegerType,true),
    StructField("HISAL",IntegerType,true) ))
  /*i,[a[j[b,[k,c[l,d]]]]]]
  */

  val denormSchema = StructType(Seq(StructField("i",StringType,true),StructField("a",
                                                   StructType(Seq(StructField("j",StringType,true),StructField("b",
                                                   StructType(Seq(StructField("k",StringType,true),StructField("c",
                                                   StructType(Seq(StructField("l",StringType,true),StructField("d",StringType,true))),true)
                                                                 )
                                                             ),true          )
                                                                )
                                                            ),true
                                             )))
  import spark.implicits._
  val empdf = spark.read
    .schema(empSchema)
    .format("csv")
    .option("delimiter","|")
    .load("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\emp.txt")
  val deptdf = spark.read
    .schema(deptSchema)
    .format("csv")
    .option("delimiter","|")
    .load("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\dept.txt")
  val SalGradedf = spark.read
    .schema(salGradeSchema)
    .format("csv")
    .option("delimiter","|")
    .load("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\SalGrade.txt")

}
