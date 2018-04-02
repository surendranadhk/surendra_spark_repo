package org.training.spark.hadoopintegration.read

import org.training.spark.apiexamples.serialization.SalesRecord
import org.training.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object ObjectFileRead {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val dataRDD = sc.objectFile[SalesRecord](args(1))
    println(dataRDD.collect().toList)

  }

}
