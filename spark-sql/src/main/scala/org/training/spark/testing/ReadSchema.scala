package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Created by hduser on 27/6/16.
 */
object ReadSchema {

  def getSchema(schemaFile: String) = {

    val schemaString = scala.io.Source.fromFile(schemaFile).mkString
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    schema
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("csvfile")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val schema = getSchema(args(1))

    val salesRDD = sc.textFile(args(2))
    val rowRDD = salesRDD
                   .map(_.split(","))
                   .map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF = sqlContext.createDataFrame(rowRDD, schema)
    //salesDF.write.save(args(2))
    salesDF.printSchema()
    salesDF.show()
  }
}
