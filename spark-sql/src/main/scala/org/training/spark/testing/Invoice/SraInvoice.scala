package org.training.spark.testing.Invoice

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hduser on 11/7/16.
 */


object SraInvoice {

  def getSraSchema(schemaFile: String) = {

    scala.io.Source.fromFile(schemaFile).getLines().toList.map(_.trim)

  }

  def main(args: Array[String]) {


    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "csvfile", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val opts = Map("header" -> "false", "InferSchema" -> "true", "delimiter" -> "|")

    val newSchema = getSraSchema(args(2))

    val sraInvoiceDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .options(opts)
      .load(args(1))
    val oldSchema = sraInvoiceDF.schema
    println(oldSchema)
    val sraInvoiceRenamedDF = sraInvoiceDF.toDF(newSchema: _*)
    sraInvoiceRenamedDF.printSchema()
    //sraInvoiceRenamedDF.show()
    //sraInvoiceRenamedDF.registerTempTable("srainvoice")

    //val sraInovicePivotedDF = hqlContext.sql("SELECT BILL_NUM, collect_set(KSCHL_TD) from srainvoice group by BILL_NUM")
    //sraInovicePivotedDF.show()

  }

}
