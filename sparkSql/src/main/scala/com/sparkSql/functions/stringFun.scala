package com.sparkSql.functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object stringFun {
val spark = SparkSession
             .builder()
             .appName("sql")
             .master("local")
             .getOrCreate()
  spark.sparkContext.setLogLevel("off")

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Winutils\\")
    spark.sql("""select Concat("anil","_","kumar"),
                |length("length"),
                |lpad("anil",6,"*"),
                |ltrim("anil   "),
                |nvl(null,"0"),
                |regexp_replace("abcdefgabc","[a-c]","9"),
                |regexp_extract("abcdefgabc","[a-c]",0),
                |replace("abcdefgabc","a","9"),
                |upper("abcdefgabc"),
                |lower("abcdefgabc"),
                |rpad("anil",12,"*"),
                |rtrim("anil  "),
                |sha2("anil",256),
                |trim("   anil   "),
                |concat_ws("|","a","b","c"),
                |substr("anil",-5,2),
                |substring_index("anila","a",3),
                |format_string("Hello World %d %s", 100, "days"),
                |string(1279),
                |translate("abcdefgabc","abc","012"),
                |md5("anil"),
                | split("anil","") """.stripMargin).show(false)
  }

}
