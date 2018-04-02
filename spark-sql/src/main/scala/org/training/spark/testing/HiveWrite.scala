package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 14/5/16.
 */
object HiveWrite {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(args(0), "CreateHiveTable", conf)

    System.setProperty("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val salesDF = hqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("InferSchema", "true").load(args(1))
    salesDF.registerTempTable("sales")
    salesDF.write.saveAsTable("lava.Sales")

    //val ebaydf = hqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("InferSchema", "true").load(args(2))
    //ebaydf.registerTempTable("ebay")


    hqlContext.sql("create table salesTable as select transactionId, customerId, itemId, amountPaid from sales")
    //hqlContext.sql("create table ebaytable as select auctionid, bid, bidtime, bidder, bidderrate, openbid, price from ebay")

  }
  }
