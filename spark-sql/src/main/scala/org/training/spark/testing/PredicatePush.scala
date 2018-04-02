package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 29/7/16.
 */
object PredicatePush {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce", "dbtable" -> "sales","user"->"root","password"->"training")
    //val mysqlOption = Map("url" -> "jdbc:mysql://hduser:3306/ecommerce", "dbtable" -> "sales","user"->"hduser","password"->"hadoop123")

    val jdbcDF = sqlContext.read.format("org.apache.spark.sql.jdbc").options(mysqlOption).load().where("amountPaid > 2500.0")

    jdbcDF.printSchema()
    jdbcDF.show()
  }
}
