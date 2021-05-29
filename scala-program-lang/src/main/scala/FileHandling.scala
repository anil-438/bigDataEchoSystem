object FileHandling {
  import java.io._
  val fileObject = new File("ScalaFile.txt" )     // Creating a file
  val printWriter = new PrintWriter(fileObject)       // Passing reference of file to the printwriter
  printWriter.write("Hello, This is scala file")  // Writing to the file
  printWriter.close()

}

import scala.io.Source
object MainObject{
  def main(args:Array[String]){
    val filename = "ScalaFile.txt"
    val fileSource = Source.fromFile(filename)
    while(fileSource.hasNext){
      println(fileSource.next)
    }
    fileSource.close()
  }
}

import scala.io.Source
object readEachLine{
  def main(args:Array[String]){
    val filename = "ScalaFile.txt"
    val fileSource = Source.fromFile(filename)
    for(line<-fileSource.getLines){
      println(line)
    }
    fileSource.close()
  }
}