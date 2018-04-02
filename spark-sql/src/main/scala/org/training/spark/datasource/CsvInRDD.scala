package org.training.spark.datasource

import org.training.spark.utils.Sales
import org.apache.spark.{SparkConf, SparkContext}

object CsvInRDD {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("csv in rdd")
    val sc  = new SparkContext(conf)
    val salesRDD  = sc.textFile(args(1))
    val firstLine = salesRDD.first()
    val otherLines = salesRDD.filter(row => row != firstLine)
    val sales = otherLines.map(_.split(",")).map(p=> Sales(p(0).trim.toInt,
      p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))
    println(sales.map(value => value.customerId).collect().toList)
  }

}
