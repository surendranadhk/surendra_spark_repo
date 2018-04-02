package org.training.spark.loganalysis.utils

import java.io.{File, FileReader, BufferedReader}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
 * Created by pramod on 26/12/15.
 */
object AccessLogUtils {

  def getHour(dateTime: String): Int = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d: Date = sdf.parse(dateTime)
    d.getHours
  }

  def getMinPattern(dateTime: String): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ddf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    ddf.format(sdf.parse(dateTime))
  }



  def toDate(sourceDate: String, srcPattern: String, destPattern: String): String = {
    val srcdf = new SimpleDateFormat(srcPattern)
    val destdf = new SimpleDateFormat(destPattern)
    val d = srcdf.parse(sourceDate)
    val result = destdf.format(d)
    result
  }

  def geProducts(requestURI: String): String = {
    val parts = requestURI.split("/")
    parts(3)
  }

  def createCountryCodeMap(filename: String): mutable.HashMap[String, String] = {

    val countryCodeMap = mutable.HashMap[String, String]()
    val bufferedReader = new BufferedReader(new FileReader(new File(filename)))
    var line: String = null
    while ({
      line = bufferedReader.readLine
      line
    } != null) {

      val pair = line.split(" ")
      countryCodeMap.put(pair(0), pair(1))
    }
    bufferedReader.close()
    countryCodeMap
  }

  def getCountryCode(ipaddress: String, cntryCodeBroadcast: Broadcast[mutable.HashMap[String, String]]): String = {
    val octets = ipaddress.split("\\.")
    val classABIP = octets(0) + "." + octets(1)

    //cntryCodeBroadcast.value(classABIP)
    val cntryMap = cntryCodeBroadcast.value
    val b = cntryMap.get(classABIP) match  {
      case Some(value) => value
      case None => "unknown"
    }
    b
  }

  def getViewedProducts(access_log: ApacheAccessLog, cntryCodeBroadcast: Broadcast[mutable.HashMap[String, String]]): ViewedProduts = {

    new ViewedProduts(getCountryCode(access_log.ipAddress, cntryCodeBroadcast),
      access_log.userId,
      toDate(access_log.dateTime,"dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss"),
      geProducts(access_log.requestURI),
      access_log.responseCode,
      access_log.contentSize)
  }
}