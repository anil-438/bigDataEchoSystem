package spark.dataFrame


import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.io.Source
//parquet,avro,json,csv and orc file will have infer schema --> .option("header","true")
object Schema extends Serializable {

  def structType: StructType ={

    val schemaString = "name age"
    val fields  = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
   StructType(fields) //StructType(StructField(name,StringType,true), StructField(age,StringType,true))

  }
  def seq: Seq[String] ={
      Seq("name", "age")
  }
  case class Person(name: String, age: Long)
  def structtype_from_json: StructType ={

    val schemaSource = Source.fromFile(new File("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\schema.json")).getLines.mkString
    DataType.fromJson(schemaSource).asInstanceOf[StructType]

  }
  def array_and_map_type: Unit ={
  val arrayStructureData = Seq(
    Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
    Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
    Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
    Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("hobbies", ArrayType(StringType))
    .add("properties", MapType(StringType,StringType))

}
  /*def conver_case_class_into_structType: Unit ={

    case class Name(first:String,last:String,middle:String)
    case class Employee(fullName:Name,age:Integer,gender:String)

    import org.apache.spark.sql.catalyst.ScalaReflection
    val schema = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]

    val encoderSchema = Encoders.product[Employee].schema
    encoderSchema.printTreeString()
  }*/
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("read_and_write_hdfs").master("local").getOrCreate()
    val structureData = Seq(
      Row(Row("James ","","Smith"),"36636","M",3100),
      Row(Row("Michael ","Rose",""),"40288","M",4300),
      Row(Row("Robert ","","Williams"),"42114","M",1400),
      Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )
    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structtype_from_json)
    df3.repartition(1)
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\Array.json")
  }

}
