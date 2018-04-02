package org.training.spark.apiexamples.errorhandling

import org.training.spark.apiexamples.serialization.{SalesRecord, SalesRecordParser}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object HandleMalformedRecordsCollect {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))

    val validatedRDD = dataRDD.map(row => SalesRecordParser.parse(row))

    validatedRDD.cache()
    val malformedRecords = validatedRDD.collect({
      case t if t.isLeft => t.left.get
    })

    val normalRecords = validatedRDD.collect({
      case t if t.isRight => t.right.get
    })

    //val normalRecords = validatedRDD.map(_._2).subtract(malformedRecords)

    //val salesRecordRDD = normalRecords.map(row => SalesRecordParser.parse(row).right.get)

    println(malformedRecords.collect().toList)
    println(normalRecords.collect().toList)

  }

}
