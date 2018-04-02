package org.training.spark.database

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 30/8/15.
 */
object MySqlRead {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc_read")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce", "dbtable" -> "sales","user"->"root", "password"->"training")
    //val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce", "dbtable" -> "sales","user"->"hduser","password"->"training")

    val jdbcDF = sqlContext.read.format("org.apache.spark.sql.jdbc").options(mysqlOption).load()
    jdbcDF.printSchema()
    jdbcDF.show()
  }
}
