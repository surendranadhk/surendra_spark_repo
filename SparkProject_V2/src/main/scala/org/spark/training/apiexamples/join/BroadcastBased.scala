package org.spark.training.apiexamples.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * Created by hduser on 15/9/16.
 */
object BroadcastBased {

  def main(args: Array[String]) {

    val log = Logger.getLogger(getClass.getName)


    val sparkSession = SparkSession.builder
      .master(args(0))
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._



    log.warn("Spark Session created")


    val salesDs = sparkSession.read.option("header","true").option("inferSchema","true").csv(args(1)).as[Sales]


    val customerDs = sparkSession.read.option("header","true").option("inferSchema","true").csv(args(2)).as[Customer]

    log.warn("Spark Session read both dataset")
    val joinedDs = salesDs.join(broadcast(customerDs), "customerId")

    log.warn("Spark Session did join")
    joinedDs.show()
  }
}
