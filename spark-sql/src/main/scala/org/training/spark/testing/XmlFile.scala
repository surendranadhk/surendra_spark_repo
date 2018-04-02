package org.training.spark.testing

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * Created by hduser on 13/8/16.
 */
object XmlFile {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "xml file", sparkConf)
    val sqlContext = new SQLContext(sc)
    val xmlOptions = Map("rowTag" -> "person")
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .options(xmlOptions)
      .load(args(1))
    df.printSchema()

    df.show()

    //import sqlContext.implicits._
    df.select("age.#VALUE, age.@born").show()
  }


}
