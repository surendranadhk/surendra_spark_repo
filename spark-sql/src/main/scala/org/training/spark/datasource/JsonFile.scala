package org.training.spark.datasource

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 15/2/15.
 */
object JsonFile {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("json file")
    val sc : SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    ///val sales = sqlContext.read.format("org.apache.spark.sql.json").load(args(1))
    val sales = sqlContext.read.json(args(1))

    sales.printSchema()

    sales.show()

  }

}
