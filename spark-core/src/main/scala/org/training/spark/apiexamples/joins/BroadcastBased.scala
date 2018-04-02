package org.training.spark.apiexamples.joins

import java.io.{File, FileReader, BufferedReader}

import org.training.spark.apiexamples.serialization.SalesRecordParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable

/**
 * Created by Arjun on 20/1/15.
 */
object BroadcastBased {


  def creatCustomerMap(customerDataPath: String) = {

    val customerMap = mutable.Map[String, String]()

    val lines = scala.io.Source.fromFile(customerDataPath).getLines()
    while (lines.hasNext) {
      val values = lines.next().split(",")
    //for(line <- lines) {
      //val values = line.split(",")
      customerMap.put(values(0), values(1))
    }
    customerMap
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val salesRDD = sc.textFile(args(1))
    val customerDataPath = args(2)
    val customerMap = creatCustomerMap(customerDataPath)

    //broadcast data

    val customerBroadCast = sc.broadcast(customerMap)

    val joinRDD = salesRDD.map(rec => {
      val salesRecord = SalesRecordParser.parse(rec).right.get
      val customerId = salesRecord.customerId
      val custMap = customerBroadCast.value
      val customerName = custMap.get(customerId) match {
        case None => "Unknonw User"
        case Some(custName) => custName
      }
      (customerName,salesRecord)
    })

    println(joinRDD.collect().toList)

  }

}
