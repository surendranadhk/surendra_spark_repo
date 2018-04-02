package org.training.spark.dataframe

import org.training.spark.utils.Sales
import org.apache.spark._

/**
 * Creating Dataframe from case classes
 */
object CaseClass {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("caseclass")
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val rawSales = sc.textFile(args(1))
    val salesDF = rawSales.filter(line => !line.startsWith("transactionId"))
      .map(_.split(","))
      .map(p => Sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))
      .toDF()


    //val salesPair = sales.map(rec => (rec.itemId, rec.amountPaid))


    //val salesDF = sqlContext.createDataFrame(salesRdd)
    salesDF.printSchema()
    //salesDF.show()

    val salesRdd = salesDF.rdd

    salesRdd

  }
}
