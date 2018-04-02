package org.training.spark.hadoopintegration.write

import org.training.spark.apiexamples.serialization.SalesRecordParser
import org.training.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by Arjun on 20/1/15.
 */
object SequenceFilePersist {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("hadoopintegration")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val salesRecordWritableRDD = salesRecordRDD.map(salesRecord => {
      (NullWritable.get(), new SalesRecordWritable(salesRecord.transactionId, salesRecord.customerId,
        salesRecord.itemId, salesRecord.itemValue))
    })

    salesRecordWritableRDD.saveAsSequenceFile(outputPath)
  }


}
