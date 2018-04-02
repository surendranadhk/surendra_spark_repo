package org.training.spark.datasource

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
 * Mixed sources
 */
object MixSources {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Mixed loading example").setMaster(args(0))
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val path = args(1)
    val fmt = args(2)
    val header = Try(args(3)).getOrElse("true")
    val option = Map("path" -> path, "header" -> header, "InferSchema" -> "true")
    val df = sqlContext.read.format(fmt).options(option).load()
    df.printSchema()
    df.show()
    //val salesRdd = df.rdd
  }
}
