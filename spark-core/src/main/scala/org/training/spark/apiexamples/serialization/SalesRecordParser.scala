package org.training.spark.apiexamples.serialization

/**
 * Created by Arjun on 20/1/15.
 */
object SalesRecordParser {

  def parse(record:String): Either[MalformedRecordException,SalesRecord] = {
    val columns = record.toString.split(",")
    if (columns.length == 4) {
      val transactionId: String = columns(0)
      val customerId: String = columns(1)
      val itemId: String = columns(2)
      val itemValue: Double = columns(3).toDouble
      Right(SalesRecord(transactionId, customerId, itemId, itemValue))
    }
    else {
      Left(new MalformedRecordException())
    }
  }

}
