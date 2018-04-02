package CRItesting

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.File

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.training.spark.testing.CRITransactionLogFile

/**
  * Created by hduser on 11/15/16.
  */
object CRIFileProcessor {

  def main(args: Array[String]): Unit = {

    val dir = new File("/home/hduser/CRI_Converstion/C20388603001-b5aebd0921e23328-1477969027983_prd")

    def getListOfFiles(dir: File): List[File] = dir.listFiles().filter(x => x.isFile && x.canRead).toList
    val lstFiles = getListOfFiles(dir)

    val conf = new SparkConf().setMaster("local").setAppName("log analysis")
    val sc = new SparkContext(conf)

    System.setProperty("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    val hiveContext = new HiveContext(sc)

    for (eachFile <- lstFiles) {
      if (eachFile.getName.endsWith(".manifest"))
        processManifestFile(eachFile, hiveContext)
      else
        processLogFile(eachFile, hiveContext)
    }

  }

  def processLogFile(file:File, hiveContext: HiveContext): Unit = {

    val lines = scala.io.Source.fromFile(file).getLines().toList

    val l = new ListBuffer[CRITransactionLogFile]()
    var i = 0

    val max_df = hiveContext.sql("select max(txnlog_seqnum) from cri_transaction_log")
    max_df.show

    var txn_seqnum = max_df.head().getLong(0)
    println("transaction_seqnum is:" + txn_seqnum)

    import ImplicitStrip._
    if ("(?i)Version: 2.0".r.findFirstIn(lines(0)).nonEmpty){
      while(i < lines.length) {
        if("(?i)TransactionLogVer".r.findFirstIn(lines(i)).nonEmpty) {
          txn_seqnum = txn_seqnum + 1

          val ct = new CRITransactionLogFile(txn_seqnum, "", "", "", "", 0, 0, 0, "", 0, 0, 0, 0, 0, "", 0, 0, 0, "", 0, "", "", "", "", "", "", "",
            "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")

          ct.source_log_file = file.getName
          ct.cluster_folder_name = file.getParentFile.getName
          ct.source_log_evn = ct.cluster_folder_name.split("_")(1).trim
          ct.batch_id = ct.cluster_folder_name.split("-")(1).trim

          println(file.getName)

          var record = lines(i-1).split("moduled")
          if("\\+".r.findFirstIn(record(0)).nonEmpty)
            ct.appliance_ts_utc = record(0).substring(1)
          else
            ct.appliance_ts_utc = record(0).substring(1)

          ct.ephemeral_hmac = record(1).split("-")(2).split("]")(0).trim

          while(!(lines(i).startsWith("[") || i == lines.length-1)){
            if("(?i)ApplianceVersion".r.findFirstIn(lines(i)).nonEmpty)
              ct.appliance_version = lines(i).trim.replace(",", "").split(":")(1).strip
            if("(?i)ClientIP".r.findFirstIn(lines(i)).nonEmpty)
              ct.client_ip = lines(i).trim.replace(",", "").split(":")(1).strip
            if("(?i)TransactionID".r.findFirstIn(lines(i)).nonEmpty) {
              ct.transaction_id = lines(i).trim.replace(",", "").split(":")(1).strip
              ct.appliance_id = "A" + "[AT]".r.split(ct.transaction_id)(1)
            }

            if("(?i)Result".r.findFirstIn(lines(i)).nonEmpty) {
              while(!(("}".r.findFirstIn(lines(i)).nonEmpty) && lines(i).trim.length < 3)) {
                if("(?i)value".r.findFirstIn(lines(i)).nonEmpty)
                  ct.result_value = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)resultCode".r.findFirstIn(lines(i)).nonEmpty) {
                  val result = lines(i).trim.replace(",", "").split(":")(1).strip
                  //println(result + " " + result.substring(2))
                  ct.result_code = if(result.startsWith("0x")) result.substring(2).trim.toInt else result.toInt
                }
                i = i +1
              }
            }

            if("(?i)TransactionStart".r.findFirstIn(lines(i)).nonEmpty)
              ct.transaction_start_org = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)TransactionEnd".r.findFirstIn(lines(i)).nonEmpty)
              ct.transaction_end_org = lines(i).trim.replace(",", "").split(":")(1).trim.toLong

            if("(?i)CMLVersion".r.findFirstIn(lines(i)).nonEmpty)
              ct.cml_version = lines(i).trim.replace(",", "").split(":")(1).strip
            if("(?i)CMLDataVer".r.findFirstIn(lines(i)).nonEmpty)
              ct.cml_data_ver = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)InstanceID".r.findFirstIn(lines(i)).nonEmpty)
              ct.instance_id = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)InstanceLogCounter".r.findFirstIn(lines(i)).nonEmpty)
              ct.instance_log_counter = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)SessionID".r.findFirstIn(lines(i)).nonEmpty)
              ct.session_id = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)SessionLogCounter".r.findFirstIn(lines(i)).nonEmpty)
              ct.session_log_counter = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)UserData".r.findFirstIn(lines(i)).nonEmpty) {
              i = i + 1
              var eachLine = ""
              var concatRecord = "\""
              while(!(("}".r.findFirstIn(lines(i)).nonEmpty) && lines(i).trim.length < 3)) {
                eachLine = lines(i).trim
                concatRecord += eachLine + ";"
                if("(?i)qXCoord".r.findFirstIn(lines(i)).nonEmpty)
                  ct.q_x_coord = lines(i).trim.replace(",", "").split(":")(1).strip.toInt
                if("(?i)qYCoord".r.findFirstIn(lines(i)).nonEmpty)
                  ct.q_y_coord = lines(i).trim.replace(",", "").split(":")(1).strip.toInt
                if("(?i)qWaferId".r.findFirstIn(lines(i)).nonEmpty)
                  ct.q_wafer_id = lines(i).trim.replace(",", "").split(":")(1).strip.toLong
                if("(?i)qCMLibVer".r.findFirstIn(lines(i)).nonEmpty)
                  ct.appliance_id = lines(i).trim.replace(",", "").split(":")(1).trim.split("\"")(1)
                i = i + 1
              }
              ct.user_data = concatRecord + "\""
            }

            if("(?i)RequestedModuleName|RequestedModuledName".r.findFirstIn(lines(i)).nonEmpty)
              ct.requested_module_name= lines(i).trim.replace(",", "").split(":")(1).trim
            if("(?i)ModuleVersion".r.findFirstIn(lines(i)).nonEmpty)
              ct.module_version = lines(i).trim.replace(",", "").split(":")(1).trim.toLong
            if("(?i)DeviceID".r.findFirstIn(lines(i)).nonEmpty) {
              val deviceId = lines(i).trim.replace(",", "").split(":")(1).strip
              ct.device_id = if(deviceId.startsWith("0x")) deviceId.substring(2).toUpperCase else deviceId.toUpperCase
              ct.product = ct.device_id.substring(0, 8)
            }

            if("(?i)\"\\\"HSMLog\\\": \\{".r.findFirstIn(lines(i)).nonEmpty) {
              while(!(("}".r.findFirstIn(lines(i)).nonEmpty) && lines(i).trim.length < 3)){
                if("(?i)encoding".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_encoding = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)value".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_value = lines(i).trim.replace(",", "").split(":")(1).strip
                i = i + 1
              }
            }

            if("(?i)HSMLogData".r.findFirstIn(lines(i)).nonEmpty) {
              while(!(("}".r.findFirstIn(lines(i)).nonEmpty) && lines(i).trim.length < 3)){
                if("(?i)Chip Series".r.findFirstIn(lines(i)).nonEmpty) {
                  val series = lines(i).trim.replace(",", "").split(":")(1).strip
                  ct.hsmlog_chip_series = if(series.startsWith("0x")) series.substring(2).toUpperCase else series.toUpperCase
                }
                if("(?i)Device Id".r.findFirstIn(lines(i)).nonEmpty) {
                  val deviceId = lines(i).trim.replace(",", "").split(":")(1).strip
                  ct.hsmlog_device_id = if(deviceId.startsWith("0x")) deviceId.substring(2).toUpperCase else deviceId
                }
                if("(?i)Module Name".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_module_name = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)Module Version".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_module_version = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)Provisioning Authority Device AES Key Id".r.findFirstIn(lines(i)).nonEmpty) {
                  val keyId = lines(i).trim.replace(",", "").split(":")(1).strip
                  ct.hsmlog_prov_auth_dvc_key_id = if(keyId.startsWith("0x")) keyId.substring(2).toUpperCase else keyId.toUpperCase
                }
                if("(?i)serialization_cvdakey_padakey".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_serial_cvdakey_padakey = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)TICKET_".r.findFirstIn(lines(i)).nonEmpty) {
                  val ticket_id = lines(i).trim.replace(",", "").split(":")(0).strip //ticket_id
                  val ticket_value = lines(i).trim.replace(",", "").split(":")(1).trim //ticket_value
                }
                if("(?i)Debug Unlock Mask".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_debug_unlock_mask = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)Debug Unlock Value".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_debug_unlock_value = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)volatile_debug_unlock".r.findFirstIn(lines(i)).nonEmpty)
                  ct.hsmlog_volatile_debug_unlock = lines(i).trim.replace(",", "").split(":")(1).strip
                if("(?i)KSV".r.findFirstIn(lines(i)).nonEmpty) {
                  val ksv = lines(i).trim.replace(",", "").split(":")(1).strip
                  ct.hsmlog_hdcp_ksv = if(ksv.startsWith("0x")) ksv.substring(2).toUpperCase else ksv.toUpperCase
                }

                if("(?i)HSMMac".r.findFirstIn(lines(i)).nonEmpty) {
                  while (!(("}".r.findFirstIn(lines(i)).nonEmpty) && lines(i).trim.length < 3)) {
                    if("(?i)encoding".r.findFirstIn(lines(i)).nonEmpty)
                      ct.hsmmac_encoding = lines(i).trim.replace(",", "").split(":")(1).strip
                    if("(?i)value".r.findFirstIn(lines(i)).nonEmpty)
                      ct.hsmmac_value = lines(i).trim.replace(",", "").split(":")(1).strip
                    i = i + 1
                  }
                }
                i = i + 1
              }
            }

            i = i + 1
            //println(i)
          }

          l += ct
        }
        i = i + 1
      }

      //val schema = StructType(Array(StructField("txn_seqnum", IntegerType, true)))

      val rdd = hiveContext.sparkContext.parallelize(l)
      //rdd.foreach(println)
      val df = hiveContext.createDataFrame(rdd)
      df.printSchema
      df.show
      df.registerTempTable("temp_transaction")

      val critxn_df = hiveContext.sql("select *, cast(FROM_UNIXTIME(transaction_start_org) as timestamp) as TRANSACTION_START_UTC, cast(FROM_UNIXTIME(transaction_end_org) as timestamp) as TRANSACTION_END_UTC from temp_transaction")
      critxn_df.printSchema()
      critxn_df.show()
      critxn_df.write.mode("append").saveAsTable("cri_transaction_log")

    }

  }

  // using implicits to extend String class to add a method strip
  object ImplicitStrip {

    implicit class Strip(str: String) {
      def strip() = str.stripSuffix("\"").stripMargin('"')
    }

  }

  // process manifest file
  def processManifestFile(file: File, hiveContext: HiveContext): Unit = {

    val lines = scala.io.Source.fromFile(file).getLines().toList

    val max_df = hiveContext.sql("select max(manifest_seqnum) from cri_manifest")
    max_df.show

    val manifest_seqnum = max_df.head().getLong(0)
    println("mainfest_seqnum is:" + manifest_seqnum)

    val l = new ListBuffer[CRIManifestFile]()
    val cm = CRIManifestFile(manifest_seqnum, "", "", "", "", 0, 0, "")
    var i = 0
    val logMap = new mutable.LinkedHashMap[String, String]

    while(i < lines.length) {
      var line = lines(i)
      if ("(?i)batchId".r.findFirstMatchIn(line).nonEmpty)
        cm.batch_id = line.split(":")(1).trim.replace(",","").replace("\"","")
      if ("(?i)clusterId".r.findFirstMatchIn(line).nonEmpty)
        cm.cluster_id = line.split(":")(1).trim.replace(",","").replace("\"","")
      if ("(?i)ClientTotalCount".r.findFirstMatchIn(line).nonEmpty)
        cm.client_log_count = (line.split(":")(1).trim.replace(",","").replace("\"","")).toLong
      if ("(?i)TransactionTotalCount".r.findFirstMatchIn(line).nonEmpty)
        cm.transaction_log_count = (line.split(":")(1).trim.replace(",","").replace("\"","")).toLong
      if ("(?i)logFilesGroupedByAppliances".r.findFirstMatchIn(line).nonEmpty) {
        i = i + 1
        while(!lines(i).startsWith("  },")){
          cm.appliance_id = lines(i).split(":")(0).trim.replace("\"","")
          val loglidt = lines(i).split(":")(1).split("\"")
          for(eachlog <- loglidt)
            if(eachlog.endsWith(".log")) {
              cm.filename = eachlog
              logMap.put(cm.filename, cm.appliance_id)
            }
          i = i + 1
        }
      }
      i = i + 1
    }

    for(eachLog <- logMap) {
      cm.manifest_seqnum = cm.manifest_seqnum+1
      val cm_log = CRIManifestFile(cm.manifest_seqnum, cm.batch_id, cm.cluster_id, eachLog._2, eachLog._1, cm.transaction_log_count, cm.client_log_count, file.getName)
      l += cm_log
    }

    val rdd = hiveContext.sparkContext.parallelize(l)
    val df = hiveContext.createDataFrame(rdd)
    df.registerTempTable("temp_mainfest")

    val cri_df = hiveContext.sql("select *, cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as timestamp) as ETL_LOAD_TS_UTC from temp_mainfest")
    //cri_df.printSchema()
    //cri_df.show()
    cri_df.write.mode("append").saveAsTable("cri_manifest")

  }
}
