name := "spark-sql"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"

//libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.3.3"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M3"
    