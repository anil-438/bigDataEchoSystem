import java.io.File

import org.apache.hadoop.fs._

object hdfs_config {
  val spark = source.spark
  import spark.implicits._
  val hc = source.sc.hadoopConfiguration
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
