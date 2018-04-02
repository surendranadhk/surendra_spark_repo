package org.training.spark.apiexamples.advanced

import org.training.spark.apiexamples.serialization.{SalesRecord, SalesRecordParser}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by madhu on 28/1/15.
 */
object Fold {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("apiexample").setMaster(args(0))
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })


    //give me salesRecord which has max value
    val dummySalesRecord = new SalesRecord(null,null,null,Double.MinValue)

    val maxSalesRecord = salesRecordRDD.reduce((acc,salesRecord)=>{
      if(acc.itemValue < salesRecord.itemValue) salesRecord else acc
    })

    println("max sale record is "+ maxSalesRecord)
  }

}
