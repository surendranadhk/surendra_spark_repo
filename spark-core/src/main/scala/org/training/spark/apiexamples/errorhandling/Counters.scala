package org.training.spark.apiexamples.errorhandling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.training.spark.apiexamples.serialization.SalesRecordParser

/**
 * Created by Arjun on 20/1/15.
 */
object Counters {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val malformedRecords = sc.accumulator(0)


     //println("Partitions: "  + dataRDD.partitions.length)
    // foreach is an action

   // dataRDD.saveAsTextFile(args(2))
    dataRDD.foreach(record => {
      val parseResult = SalesRecordParser.parse(record)
      if(parseResult.isLeft){
        malformedRecords += 1
      }
    })


    //dataRDD.foreach(println(_))
    //print the counter

    println("No of malformed records is =  " + malformedRecords.value)


  }

}
