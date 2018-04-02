package org.training.spark.database

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode

import scala.collection.immutable.Map
import scala.collection.JavaConverters._



/**
 * Created by hduser on 31/8/15.
 */
object MySqlWrite {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc_write")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(args(1))

    //val option = Map("url"->"jdbc:mysql://localhost:3306/ecommerce","dbtable"->"sales")

    val properties:Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","training")

    salesDf.printSchema()
    salesDf.show()

    salesDf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/ecommerce","sales1",properties)

  }
}
