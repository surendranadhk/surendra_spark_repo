package org.training.spark.testing

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 29/7/16.
 */
object PredicatePush1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val properties:Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","training")

    val where = "(select customerId from sales where amountPaid > 2500.0) as temp"
    val jdbcDF = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/ecommerce", where, properties)

    jdbcDF.printSchema()
    jdbcDF.show()
  }
}
