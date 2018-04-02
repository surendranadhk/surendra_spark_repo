name := "LogAnalyzer"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.1"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.3"