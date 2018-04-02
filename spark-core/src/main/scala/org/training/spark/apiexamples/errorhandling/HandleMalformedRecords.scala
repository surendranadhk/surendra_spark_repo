package org.training.spark.apiexamples.errorhandling

import org.apache.spark.storage.StorageLevel
import org.training.spark.apiexamples.serialization.{SalesRecord, SalesRecordParser}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object HandleMalformedRecords {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))

    val parsedRdd = dataRDD.map(record => {
      val parseResult = SalesRecordParser.parse(record)
      if(parseResult.isRight){
        (true,parseResult.right.get)
      }
      else (false,record)
    }).persist(StorageLevel.MEMORY_AND_DISK)


    val malformedRecords = parsedRdd.filter(_._1 == false).map(_._2)
     val normalRecords = parsedRdd.filter(_._1 == true).map(x => x._2 match {
      case y:SalesRecord => y
    }).cache


    //val normalRecords1 = parsedRdd.map(_._2).subtract(malformedRecords)

    //val salesRecordRDD = normalRecords.map(row => SalesRecordParser.parse(row).right.get)

    println(malformedRecords.collect().toList)
    println(malformedRecords.count())
    println(normalRecords.collect().toList)

  }

}
