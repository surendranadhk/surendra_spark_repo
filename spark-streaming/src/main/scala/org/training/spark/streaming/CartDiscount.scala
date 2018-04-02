package org.training.spark.streaming

import org.training.spark.streaming.serialization.SalesRecordParser
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
Maintain wordcount across multiple batches
  * * Takes
  *  args(0) - master Url.
  *  args(1) - hostname of machine that has stream
  *  args(2) - port
  *  args(3) - check point directory
 */
object CartDiscount {

  def main(args: Array[String]) {

    def updateFunction(rows:Seq[Double], runningValue:Option[Double]) = {
      val newValue = rows.sum + runningValue.getOrElse(0.0)
      Some(newValue)
    }

    val ssc = new StreamingContext(args(0), "cartdiscount", Seconds(10))

    val networkStream = ssc.socketTextStream(args(1),args(2).toInt)

    ssc.checkpoint(args(3))

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
    val eligibleCustomers = cartValueByCustomer.filter(_._2 > 500)
    eligibleCustomers.map(_._1).print()

    ssc.start()
    ssc.awaitTermination()


  }


}
