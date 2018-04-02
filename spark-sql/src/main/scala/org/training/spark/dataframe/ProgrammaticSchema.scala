package org.training.spark.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}


object ProgrammaticSchema {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("programmaticschema")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesRDD = sc.textFile(args(1))

    val schema =
      StructType(
        Array(StructField("transactionId", IntegerType, true),
          StructField("customerId", IntegerType, true),
          StructField("itemId", IntegerType, true),
          StructField("amountPaid", DoubleType, true))
      )

    val testRDD = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "false").option("mode", "DROPMALFORMED").schema(schema).load(args(1))

    testRDD.select("transactionId","amountPaid").show
    /*val rowRDD = salesRDD.filter(line => !line.startsWith("transactionId"))
      .map(_.split(",")).map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF = sqlContext.createDataFrame(rowRDD, schema)
    //salesDF.write.save(args(2))
    salesDF.printSchema()
    salesDF.show()*/
  }

}
