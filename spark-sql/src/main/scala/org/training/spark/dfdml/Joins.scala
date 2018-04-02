package org.training.spark.dfdml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by hduser on 26/8/15.
 */
object Joins {

  def main(args: Array[String]) {
    //Register the table - sales
    val conf = new SparkConf().setMaster(args(0)).setAppName("joins")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    //Register the table customer
    val customerDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(2))
    customerDf.cache()

    joinQuery(salesDf, customerDf)
    nestedQuery(salesDf, customerDf)
  }

  def joinQuery(salesDf:DataFrame, customerDf:DataFrame){
    //Join two tables

    val customerJoin = salesDf.join(customerDf, salesDf("customerId") === customerDf("customerId"), "inner")
    //val customerJoinSales = salesDf.join(customerDf, salesDf.col("customerId").equalTo(customerDf.col("customerId")))
    customerJoin.show()
  }

  def nestedQuery(salesDf:DataFrame, customerDf:DataFrame){
    //Nested query
  val customerWiseSaleAmount = salesDf.groupBy("customerId").agg(sum("amountPaid") as "totalAmount")
  val nestedOutput = customerWiseSaleAmount.filter("totalAmount >= 600")
    nestedOutput.show()
  }

}
