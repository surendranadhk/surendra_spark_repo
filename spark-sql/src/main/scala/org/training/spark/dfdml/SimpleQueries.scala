package org.training.spark.dfdml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * Created by hduser on 26/8/15.
 */
object SimpleQueries {

  def main(args: Array[String]) {
88
    val conf = new SparkConf().setMaster(args(0)).setAppName("simplequeries")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val opt = Map("header" -> "true", "InferSchema" -> "true")
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").options(opt).load(args(1))
    salesDf.cache()


    //Projection
    val itemIds = salesDf.select("itemId", "customerId")
    println("Projecting itemId column from sales")
    itemIds.show()

    //Aggregation
    val totalSaleCount = salesDf.agg(("transactionId","count"))
    //val totalSaleCount = sqlContext.sql("SELECT count(*) FROM sales")
    println("Counting total number of sales")
    totalSaleCount.show()

    //Group by
    val totalAmtPerCustomer = salesDf.groupBy("customerId").agg(("amountPaid", "sum"))
    println("Customer wise sales count")
    totalAmtPerCustomer.show()

  }

}
