package org.training.spark.streaming.extend

import org.training.spark.streaming.serialization.{SalesRecord, SalesRecordParser}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
 * Created by hduser on 28/2/15.
 */
object FoldExample {

  def main(args: Array[String]) {
    val ssc = new StreamingContext(args(0), "maxsale", Seconds(30))
    val networkStream = ssc.socketTextStream(args(1),args(2).toInt)


    val cartStream = networkStream.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val zeroValue = new SalesRecord("","","",0.0)
    import ExtraFunctions._

    val maxValue = cartStream.fold(zeroValue)((acc,record) => {
      if(acc.itemValue > record.itemValue) acc else record
    })


    maxValue.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

   }
}
