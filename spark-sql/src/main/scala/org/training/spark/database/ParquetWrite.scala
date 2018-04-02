package org.training.spark.database

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Write into parquet file
 */
object ParquetWrite {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("parquet write")
    //sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
    val sc : SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    //salesDf.write.format("orc").save(args(2))
    salesDf.write.save(args(2))


  }

}
