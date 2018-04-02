package org.training.spark.testing.xml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hduser on 13/8/16.
 */
object Books {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "xml file", sparkConf)
    val sqlContext = new SQLContext(sc)
    val xmlOptions = Map("rowTag" -> "book")
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .options(xmlOptions)
      .load(args(1))
    df.printSchema()
    df.show()
  }
}
