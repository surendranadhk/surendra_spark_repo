package org.training.spark.datasource

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 26/8/15.
 */
object CsvFile {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("csvfile")
    val sc  = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //val opt = Map("header" -> "true", "InferSchema" -> "true", "delimiter" -> "|")
    val opt = Map("header" -> "true", "InferSchema" -> "true")
    //val opt = Map("InferSchema" -> "true")

    val sales = sqlContext.read
                          .format("com.databricks.spark.csv")
                          .options(opt)
                            //.option("header", "true").option("InferSchema", "true")
                          .load(args(1))
    sales.printSchema()

    sales.show()
    //sales.write.format("text").save(args(2))
    sales.write.format("com.databricks.spark.csv").options(opt).save(args(2))

  }

}
