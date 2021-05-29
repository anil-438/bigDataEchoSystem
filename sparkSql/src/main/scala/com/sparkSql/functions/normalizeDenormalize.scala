package com.sparkSql.functions
import  SourceDF._
import org.apache.spark.sql._
object normalizeDenormalize {
  def main(args: Array[String]): Unit = {
    val simpleData = spark.sparkContext.parallelize(Seq(Row("1",Row("2", Row("9", Row("13","James")))) ,
                                                        Row("6",Row("3", Row("10", Row("14","King")))),
                                                        Row("7",Row("4", Row("11", Row("15","anil")))),
                                                        Row("8",Row("5", Row("12", Row("16","anil"))))
                                                       )
                                                   )

    val sc = spark.sparkContext
    val denormdf = spark.createDataFrame(simpleData,denormSchema)
    System.setProperty("hadoop.home.dir", "C:\\Winutils\\")
    denormdf.write
      .format("json")
      .option("header",true)
      .option("delimiter","|")
      .mode(SaveMode.Overwrite)
      .save("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\denormdat.json")
    val json = spark.read.option("header",true).json("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\denormdat.json")
    denormdf.createOrReplaceTempView("denorm")
    spark.sql("""select * from denorm""").show(false)
    spark.sql("""select a.b.c.l as l from denorm""").show(false)
    json.createOrReplaceTempView("denormJson")
    spark.sql("""select * from denormJson""").show(false)
    spark.sql("""select a.b.c.l as l from denormJson""").show(false)

                                             }
                           }
