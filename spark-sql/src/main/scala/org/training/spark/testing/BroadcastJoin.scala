package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * Created by hduser on 29/7/16.
 */
object BroadcastJoin {

  def main(args: Array[String]) {

    val sparkConf =  new SparkConf().setMaster(args(0)).setAppName("BroadcastJoin")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val salesDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    //Register the table customer
    val customerDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(2))
    customerDF.cache()

    //sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "1048576000")
    salesDF.join(broadcast(customerDF), "customerId").show
  }
}
