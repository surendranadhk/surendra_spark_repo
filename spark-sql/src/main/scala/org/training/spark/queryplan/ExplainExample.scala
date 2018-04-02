package org.training.spark.queryplan

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 1/9/15.
 */
object ExplainExample {
  def main(args: Array[String]) {
    //Register the table - sales
    val conf = new SparkConf().setMaster(args(0)).setAppName("joins")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    //Register the table customer
    val customerDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(2))

    val customerJoinSales = salesDf.join(customerDf, salesDf.col("customerId").equalTo(customerDf.col("customerId"))).filter("amountPaid!=100.0").filter("amountPaid!=505.0")

    println("explain  : ")
    println(customerJoinSales.explain(true))

    println("logical  : ")
    println(customerJoinSales.queryExecution.logical)

    println("analyzed : ")
    println(customerJoinSales.queryExecution.analyzed)

    println("optimised : ")
    println(customerJoinSales.queryExecution.optimizedPlan)
  }
}
