package org.spark.training.apiexamples.join

import org.apache.spark.sql.SparkSession


/**
 * Created by hduser on 15/9/16.
 */
case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid:Double)
case class Customer(customerId: Int, customerName: String)
case class CustomerSales(customerName: String, itemId: Int, amountPaid:Double)

object ShuffleBased {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master(args(0))
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._

    val salesDs = sparkSession.read.option("header","true").option("inferSchema","true").csv(args(1)).as[Sales]

    val customerDs = sparkSession.read.option("header","true").option("inferSchema","true").csv(args(2)).as[Customer]


    // join returns a dataframe
    val joinedDf = customerDs.join(salesDs, salesDs("customerId") === customerDs("customerId"))

    // join with returns a dataset
    val joinedDs = customerDs.joinWith(salesDs, salesDs("customerId") === customerDs("customerId"))

    joinedDf
    val result = joinedDs.map{
      case (Customer(customerId, customerName), Sales(transactionId, _, itemId, amountPaid)) => {
        CustomerSales(customerName, itemId, amountPaid)
      }
    }

    result.show()
    /*
    val sc = new SparkContext(args(0), "apiexamples")
    val salesRDD = sc.textFile(args(1), 40)
    val customerRDD = sc.textFile(args(2), 4)

    val salesPair = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      (salesRecord.customerId,salesRecord)
    })

    val customerPair = customerRDD.map(row => {
      val columnValues = row.split(",")
      (columnValues(0),columnValues(1))
    })


    val joinRDD = customerPair.join(salesPair)

    val result = joinRDD.map{
      case (customerId,(customerName,salesRecord)) => {
        (customerName,salesRecord.itemId)
      }
    }

    println(result.collect().toList) // Not recommended to use colllect after join....
*/
  }


}
