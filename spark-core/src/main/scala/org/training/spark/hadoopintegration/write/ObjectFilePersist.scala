package org.training.spark.hadoopintegration.write

import org.training.spark.apiexamples.serialization.SalesRecordParser
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object ObjectFilePersist {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("hadoopintegration")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    salesRecordRDD.saveAsObjectFile(outputPath)

  }

}
