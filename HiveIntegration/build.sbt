name := "HiveIntegration"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"

libraryDependencies += "org.apache.hive" % "hive-contrib" % "1.2.1"

libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.18"
