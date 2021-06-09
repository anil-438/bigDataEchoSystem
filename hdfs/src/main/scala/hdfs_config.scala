import java.io.File


import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import  org.apache.spark.sql.SparkSession

object hdfs_config {
  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example2")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  val hc =  spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hc)
   def listStatus: Unit ={

    println( fs.listStatus(new Path("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources")).map(c => (c.getPath,c.getLen,c.getOwner)).mkString("|"))

}
   def getContentSummary: Unit ={

   }
   def exists: Unit ={
     println( fs.exists(new Path("/C:/Users/me/IdeaProjects/bigData/src/main/resources/people.csdv")) )

   }

  def main(args: Array[String]): Unit = {
    exists
  }
}
