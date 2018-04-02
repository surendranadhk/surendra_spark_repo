package org.training.spark.apiexamples.serialization

/**
 * Created by Arjun on 20/1/15.
 */
case class SalesRecord(val transactionId: String,
                  val customerId: String,
                  val itemId: String,
                  val itemValue: Double)
{

/*
  override def toString: String = {
    transactionId+","+customerId+","+itemId+","+itemValue
  } */
}
