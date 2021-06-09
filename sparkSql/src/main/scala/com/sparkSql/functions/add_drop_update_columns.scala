package com.sparkSql.functions

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import source.{salGradeSchema, spark}
import spark_sql._

object add_drop_update_columns {

  val salGradeSchema = StructType(Seq(StructField("GRADENUMBER",StringType,true),
    StructField("LOWSAL",IntegerType,true),
    StructField("HISAL",IntegerType,true) ))

    val salgradeDF = spark.read.option("delimiter","|").schema(salGradeSchema).csv("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\column_operations.txt")

  def addNewColumn: Unit =  {
    //data type update will not create new column
    salgradeDF.withColumn("GRADE",col("GRADENUMBER").cast("Integer")).show(false)

    salgradeDF.withColumn("GRADE",col("GRADENUMBER").cast("Integer")+1).show(false)

    salgradeDF.withColumn("GRADE",lit("a")).show(false)

}
  def update_dataType: Unit =  {
    import source.spark.implicits._

salgradeDF.withColumn("GRADENUMBER",col("Gradenumber").cast("Integer")).printSchema()
  }
  def update_column_data: Unit ={
salgradeDF.withColumn("GRADENUMBER",salgradeDF("GRADENUMBER").cast("integer")+1).show(false)
  }
  def update_column_name: Unit ={

    salgradeDF.withColumnRenamed("gradenumber","grade").show(false)
  }
  def dropColumn: Unit ={
salgradeDF.drop("lowsal").show(false)
  }

  def main(args: Array[String]): Unit = {
    addNewColumn

  }
}
