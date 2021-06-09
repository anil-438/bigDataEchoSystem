package spark.dataFrame

import java.util.Properties

import org.apache.spark.sql.SparkSession

object jdbc {
  val connectionProperties = new Properties()
  def java_connection(spark:SparkSession): Unit ={
    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    // Specifying the custom data types of the read schema
    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF3 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    jdbcDF2.write
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Specifying create table column data types on write


  }
  def jdbc(spark:SparkSession): Unit ={
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    // Saving data to a JDBC source

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()

    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

  }
}
