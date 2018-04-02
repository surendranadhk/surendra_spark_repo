package org.training.spark.testing.cassandra

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 16/5/16.
 */
object CassandraRead {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val master =  args(0)
    sparkConf.setMaster(master)
    sparkConf.setAppName("SensorDataAnalytics")

    sparkConf.set("spark.cassandra.connection.host", "localhost")
    //sparkConf.set("spark.cassandra.connection.keep_alive_ms", "40000")

    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val cassandraOptions = Map( "table" -> "visits_by_country", "keyspace" -> "log.events")
    val cityStatsDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions)
      .load()

    cityStatsDF.registerTempTable("sales")

    val citydata = sqlContext.sql("Select * from sales").show()

  }
}
