package spark.dataFrame



import org.apache.spark.sql._



//DataFrames can also be saved as persistent tables into Hive metastore using the saveAsTable command.
object hive {
  System.setProperty("spark.sql.warehouse.dir", "C:\\Hive\\apache-hive-2.3.7-bin\\")
  System.setProperty("hadoop.home.dir", "C:\\Winutils\\")

  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  def saveAsTable: Unit ={

    val csv = spark.read
      .format("csv")
      .option("header","true")
      .option("delimiter",";")
      .option("mode", "DROPMALFORMED")
      .option("mergeSchema", "true")
      .load("C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\*.csv")

    csv.write.
      bucketBy(42, "name").
      sortBy("age")
      .mode(SaveMode.Overwrite)
      .saveAsTable("saveASTable")
      spark.sql(s"""select * from saveASTable""").show(false)
   }
  def internal_table: Unit ={
    // sql("CREATE TABLE IF NOT EXISTS src_01 (key STRING, value INT) USING hive")
    // sql("LOAD DATA LOCAL INPATH 'C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.txt' INTO TABLE src_01")
    // sql ("select * FROM src").show(false)
  }
  def external_table: Unit ={
spark.sql(s""" CREATE EXTERNAL TABLE IF NOT EXISTS test_ext
             | (name string,
             | age int,
             | job string
             | )
             | ROW FORMAT DELIMITED
             | FIELDS TERMINATED BY ';'
             | STORED AS TEXTFILE
             | LOCATION 'C:\\Users\\me\\IdeaProjects\\bigData\\src\\main\\resources\\people.csv'""".stripMargin)


  }
  def create_db: Unit ={

  }
  def read_data_hive: Unit ={

  }
  def write_data_hive: Unit ={

  }
  def showDB: Unit ={

  }
  def showTable: Unit ={

  }
  def DropDB: Unit ={

  }
  def dropTable: Unit ={

  }

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    import spark.sql
    external_table

  }
}
