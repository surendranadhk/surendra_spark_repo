package org.training.spark.streaming

import org.training.spark.streaming.serialization.SalesRecordParser
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hduser on 21/1/15.
 */
object RecoverableCartDiscount {

  def main(args: Array[String]) {

    def updateFunction(rows:Seq[Double], runningValue:Option[Double]) = {
      val newValue = rows.sum + runningValue.getOrElse(0.0)
      Some(newValue)
    }

    val checkPointDirectory = args(3)
    val ssc = StreamingContext.getOrCreate(checkPointDirectory,() => {
      println("new context getting started")
      val context = new StreamingContext(args(0), "recoverablecart", Seconds(10))
      context.checkpoint(checkPointDirectory)

      val networkStream = context.socketTextStream(args(1),args(2).toInt)

      /**
       * The input data is a comma separated with following columns
       *
       * transactionId,customerId,itemId,itemValue
       */

      val cartStream = networkStream.map(row => {
        val parseResult = SalesRecordParser.parse(row)
        val salesRecord = parseResult.right.get
        (salesRecord.customerId,salesRecord.itemValue)
      })

      val cartValueByCustomer = cartStream.updateStateByKey(updateFunction _)

      //find out eligible customers for given time
      val eligibleCustomers = cartValueByCustomer.filter(_._2 > 2500)
      eligibleCustomers.map(_._1).print()

      context
    })





    ssc.start()
    ssc.awaitTermination()


  }


}
