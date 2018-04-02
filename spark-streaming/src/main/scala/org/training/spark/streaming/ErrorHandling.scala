package org.training.spark.streaming

import org.training.spark.streaming.serialization.SalesRecordParser
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * * Takes
 *  args(0) - master Url.
 *  args(1) - hostname of machine that has stream
 *  args(2) - port
 *  args(3) - check point directory
 *  args(4) - errorRecords
 */
object ErrorHandling {

  def main(args: Array[String]) {


    def salesUpdateFunction(rows:Seq[Double], runningValue:Option[Double]) = {
      val newValue = rows.sum + runningValue.getOrElse(0.0)
      Some(newValue)
    }

    def erroCountUpdateFunction(rows:Seq[String], runningValue:Option[Int]) = {
      val newValue = rows.length + runningValue.getOrElse(0)
      Some(newValue)
    }


    val ssc = new StreamingContext(args(0), "ErrorHandling", Seconds(10))
    val networkStream = ssc.socketTextStream(args(1),args(2).toInt)

    ssc.checkpoint(args(3))

    val errorRecordsOutputDir = args(4)

    /**
     * The input data is a comma separated with following columns
     *
     * transactionId,customerId,itemId,itemValue
     */

    val validatedStream = networkStream.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      if(parseResult.isLeft) {
        (false,row)
      } else (true,row)
    })

    val malformedRecords = validatedStream.filter(_._1 == false).map(_._2)
    val normalRecords = validatedStream.filter(_._1 == true).map(_._2)

    val errorCount = malformedRecords.map(row => ("total error count",row)).updateStateByKey(erroCountUpdateFunction _).map(_._2)

    errorCount.print()

    //save error records

    malformedRecords.foreachRDD((rdd,time) => {
      if(rdd.count() > 0) {
        rdd.saveAsTextFile(errorRecordsOutputDir + "/" + time)
      }
    })


    val cartStream = normalRecords.map(row => {
      val columnVales = row.split(",")
      val customerId = columnVales(1)
      val itemValue = columnVales(3).toDouble
      (customerId,itemValue)
    })

    val cartValueByCustomer = cartStream.updateStateByKey(salesUpdateFunction _)

    //find out eligible customers for given time
    val eligibleCustomers = cartValueByCustomer.filter(_._2 > 500)
    eligibleCustomers.map(_._1).print()

    ssc.start()
    ssc.awaitTermination()




  }




}
