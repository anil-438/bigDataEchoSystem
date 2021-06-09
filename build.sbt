name := "bigDataEchoSystem"
scalaVersion in ThisBuild := "2.11.8"

lazy val globle = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    `scala-program-lang`,
    `spark-rdd`,
    `spark-df-ds`,
    `HBase`,
    `sparkSql`,
    `hdfs`
  )


lazy val `hdfs` = project
  .settings(
    name := "hdfs",
    settings,
    assemblySettingsSpark,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependency.spark_sql,
      dependency.spark_core,
      dependency.excel,
      dependency.excel_poi,
      dependency.hive_sql,
      dependency.jsonPath,
      dependency.kafka,
      // dependency.oracleJDBC,
      dependency.pgpPG,
      //  dependency.pgpProvider,
      dependency.postgresJDBC,
      dependency.spark_avro,
      dependency.spark_stream,
      // dependency.sqlDBConnecter,
      dependency.sqlserverJDBC,
      dependency.slf4j
    )
  ) .dependsOn(`spark-rdd`)


lazy val `scala-program-lang` = project
  .settings(
    name := "scala-program-lang",
    settings,
    assemblySettingsSpark,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependency.spark_sql,
      dependency.spark_core
    )
) .dependsOn(`spark-rdd`)



lazy val `HBase` = project
  .settings(
    name := "HBase",
    settings,
    assemblySettingsSpark,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependency.spark_sql,
      dependency.spark_core,
      "org.apache.hbase" % "hbase-server" % "1.2.1",
      "org.apache.hbase" % "hbase-client" % "1.2.1",
      "org.apache.hbase" % "hbase-common" % "1.2.1",
      "org.apache.hadoop" % "hadoop-common" % "2.7.3"
    )
  ) .dependsOn(`spark-rdd`)

lazy val `spark-df-ds` = project
  .settings(
    name := "spark-df-ds",
    settings,
    assemblySettingsSpark,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependency.spark_sql,
      dependency.spark_core,
      dependency.excel,
      dependency.excel_poi,
      dependency.hive_sql,
      dependency.jsonPath,
      dependency.kafka,
      // dependency.oracleJDBC,
      dependency.pgpPG,
      //  dependency.pgpProvider,
      dependency.postgresJDBC,
      dependency.spark_avro,
      dependency.spark_stream,
      // dependency.sqlDBConnecter,
      dependency.sqlserverJDBC,
      dependency.slf4j

    )
  ) .dependsOn(`spark-rdd`)

lazy val `sparkSql` = project
  .settings(
    name := "sparkSql",
    settings,
    assemblySettingsSpark,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependency.spark_sql,
      dependency.spark_core,
      dependency.excel,
      dependency.excel_poi,
      dependency.hive_sql,
      dependency.jsonPath,
      dependency.kafka,
      // dependency.oracleJDBC,
      dependency.pgpPG,
      //  dependency.pgpProvider,
      dependency.postgresJDBC,
      dependency.spark_avro,
      dependency.spark_stream,
      // dependency.sqlDBConnecter,
      dependency.sqlserverJDBC,
      dependency.slf4j

    )
  ) .dependsOn(`spark-rdd`)

lazy val `spark-rdd` = project
  .settings(
    name := "spark-rdd",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(

      dependency.spark_sql,
      dependency.spark_core,
      dependency.excel,
      dependency.excel_poi,
      dependency.hive_sql,
      dependency.jsonPath,
      dependency.kafka,
      // dependency.oracleJDBC,
      dependency.pgpPG,
      //  dependency.pgpProvider,
      dependency.postgresJDBC,
      dependency.spark_avro,
      dependency.spark_stream,
      // dependency.sqlDBConnecter,
      dependency.sqlserverJDBC,
      dependency.slf4j

    )
  )

lazy val dependency =
  new {
    val sparkVersion = "2.4.0"
    val slf4jV="1.7.25"

    val spark_core = "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided"
    /*If you're running from inside Intellij IDEA, and you've marked your spark library as "provided",
     like so: "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
     Then you need edit your Run/Debug configuration and check the
     "Include dependencies with Provided scope" box.*/
    val spark_sql = "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided"
    val hive_sql = "org.apache.spark" % "spark-hive_2.11" % sparkVersion % "provided"
    val excel = "org.apache.poi" % "poi-ooxml" % "3.14"
    val excel_poi = "org.apache.poi" % "poi" % "3.14"
    val spark_stream = "org.apache.spark" % "spark-streaming_2.11" % "2.4.0" % "provided"
    val kafka = "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.0" % "provided"
    val spark_avro = "org.apache.spark" % "spark-avro_2.11" % "2.4.0"
    val postgresJDBC="org.postgresql" % "postgresql"%"42.2.5"
    val pgpPG ="org.bouncycastle" %"bcpg-jdk15on"%"1.61"
    //val pgpProvider ="org.bouncycastle" %"bcproy-jdk15on"%"1.61"
    //val sqlDBConnecter = "com.microsoft.azure" %"azure-sqldb-spark"%"9.0.0-SNAPSHOT"
    //val oracleJDBC="com.oracle"%"ojdbc"%"12.1.0.2"
    val sqlserverJDBC="com.microsoft.sqlserver"%"mssql-jdbc"%"8.2.0.jre8"
    val jsonPath="com.jayway.jsonpath"%"json-path"%"2.4.0"
    val slf4j ="org.slf4j"%"jcl-over-slf4j"%slf4jV


  }

lazy val commonDependencies = Seq(
  dependency.spark_core,
  dependency.spark_sql
)

lazy val settings=commonSettings

lazy val compilerOptios = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings=Seq(
  scalacOptions++=compilerOptios,
  /*resolvers ++= Seq(
    /*"ArtifactoryRel" at "https://anil.com/artifactory/lib",
    "ArtifactorySnap" at "https://anil.com/artifactory/snapshot",*/
    "ArtifactoryRel" at "C:\\Users\\anil\\Desktop\\Big Data\\latest\\lib",
    "ArtifactorySnap" at "C:\\Users\\anil\\Desktop\\Big Data\\latest\\snapshot",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  ),*/
  updateOptions := updateOptions.value.withLatestSnapshots(false)
)

lazy val assemblySettings=Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "org.bouncycastle" => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  logLevel in assembly := Level.Error
)

lazy val assemblySettingsSpark=Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "org.bouncycastle" => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter{ f =>
      f.data.getName.contains("scala-library") || f.data.getName.contains("scala-reflect")||
        f.data.getName.contains("scala-xml") || f.data.getName.contains("commons-lang3")||
        f.data.getName.contains("slf4j")
    }
  },
  logLevel in assembly := Level.Error
)
