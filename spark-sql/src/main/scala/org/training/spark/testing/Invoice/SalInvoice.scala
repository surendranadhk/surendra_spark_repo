package org.training.spark.testing.Invoice

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 28/6/16.
 */
object SalInvoice {

  def isAllDigits(s: String) = s.forall(_.isDigit)


  def removeZeroPrefix(s: String) =  if(isAllDigits(s)) s.toInt.toString else s


  def getNewSchema(schemaFile: String) = {

    scala.io.Source.fromFile(schemaFile).getLines().toList.map(_.trim) ::: List( "c28", "c29", "c30")
    //val schemaList = scala.io.Source.fromFile(schemaFile).getLines().toList
    //val schemaItr = for (line <- lines) yield line
    //schemaItr.toList :+ "l28" :+ "l29" :+ "l30"
    //val schema = StructType(schemaList.map(fieldName => StructField(fieldName, StringType, true)))
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc  = new SparkContext(args(0), "csvfile", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val opts = Map("header" -> "false", "InferSchema" -> "true", "delimiter" -> "|")

    val newSchema = getNewSchema(args(2))

    val invoiceDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .options(opts)
      .load(args(1))
    val oldSchema = invoiceDF.schema
    println(oldSchema)
    val invoiceRenamedDF = invoiceDF.toDF(newSchema: _*)
    invoiceRenamedDF.printSchema()
    //invoiceRenamedDF.show()
    invoiceRenamedDF.registerTempTable("invoice")

    sqlContext.udf.register("removeZeroPrefix", removeZeroPrefix _)

    val ZICInvoice = sqlContext.sql("""select BILL_NUM, BILL_ITEM, BILL_TYPE , removeZeroPrefix(MATERIAL)  from invoice where BILL_TYPE = 'ZIC' """)
    ZICInvoice.show()

    val F2Invoice =  sqlContext.sql("""select BILL_NUM, BILL_ITEM, BILL_TYPE , removeZeroPrefix(MATERIAL)  from invoice where BILL_TYPE = 'F2' """)
    F2Invoice.show()

    //ZICInvoice.write.format("orc").save(args(3))

   // F2Invoice.write.save(args(4))


  }

}
