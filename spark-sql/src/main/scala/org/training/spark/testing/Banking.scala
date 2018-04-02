package org.training.spark.testing

/**
 * Created by hduser on 20/7/16.
 */

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

object Banking {

  def main(args: Array[String]) {


     val conf = new SparkConf().setAppName("sales").setMaster("local")
     val sc = new SparkContext(conf)
     val sqlcontext = new org.apache.spark.sql.SQLContext(sc)


     val bankTransaction = Source.fromURL("http://samplecsvs.s3.amazonaws.com/SalesJan2009.csv").mkString
     val writer = new PrintWriter("banktransaction.csv")
     writer.write(bankTransaction)
     writer.close()

     val opt = Map("header" -> "true", "InferSchema" -> "true")

     val sales = sqlcontext.read
      .format("com.databricks.spark.csv")
      .options(opt)
      .load("banktransaction.csv")
      //.load("http://samplecsvs.s3.amazonaws.com/SalesJan2009.csv")

     sales.printSchema()

     sales.show()

  }

}

