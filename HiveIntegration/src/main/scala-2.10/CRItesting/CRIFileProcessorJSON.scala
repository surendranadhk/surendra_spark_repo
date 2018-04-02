package CRItesting

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.joda.time.{DateTime,DateTimeZone}

/**
  * Created by hduser on 11/27/16.
  */
object CRIFileProcessorJSON extends App {

  val dir = new File("src/main/resources/C10285801001-7eb537ad7ed083-1468533155172_tst")

  def getListOfFiles(dir: File): List[File] = dir.listFiles().filter(x => x.isFile && x.canRead).toList

  val lstFiles = getListOfFiles(dir)

  val conf = new SparkConf().setMaster("local").setAppName("CRI JSON log analysis")
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
    //processLogFile(eachFile, hiveContext)
  }

  def processLogFile(file:File, hiveContext: HiveContext): Unit = {

    println("filename is:" + file.getAbsolutePath)
    val logDF = hiveContext.read.format("org.apache.spark.sql.json").option("inferSchema", "true").load(file.getAbsolutePath)

    logDF.printSchema()
    logDF.show()

    val cluster_folder_name = file.getParentFile.getName
    val source_log_evn = cluster_folder_name.split("_")(1).trim
    val batch_id = cluster_folder_name.split("-")(1).trim
    val cluster_id = cluster_folder_name.split("-")(0).trim

    val parseDateTime = udf((input: String) => {

      val date_time = new DateTime(input, DateTimeZone.UTC)
      val dateObj = date_time.toDate
      date_time.secondOfMinute()

      ////date_time.toDateTime(DateTimeZone.UTC).getMillis
      //println(date_time.toDate.toString)
      ////date_time.getMillis
      val utcString = new DateTime(dateObj).withZone(DateTimeZone.UTC).toString()
      utcString
      //date_time.toString
      //val month = if(date_time.getMonthOfYear <= 9) ("0" + date_time.getMonthOfYear) else date_time.getMonthOfYear.toString
      //val datetime_str = date_time.getYear + "-" + month + "-" + date_time.getDayOfMonth + "T" + date_time.getHourOfDay
    })

    //val txn_max_df = hiveContext.sql("select max(txn_log_seqnum) from cri_transaction_event_log")
    //txn_max_df.show

    val txn_seqnum:Long = 1//txn_max_df.head().getLong(0)
    println("transaction_seqnum is:" + txn_seqnum)

    val txn_event_df = logDF.filter(logDF("data.ClientEvent.ApplianceVersion").isNull && logDF("data.TransactionEvent.ApplianceVersion").isNotNull)

    logDF.select(logDF("data.ClientEvent"), logDF("data.ClientEvent.ApplianceVersion"), logDF("data.ClientEvent.ClientIP"), logDF("data.TransactionEvent.ApplianceVersion"), logDF("data.TransactionEvent.ClientIP"),
      logDF("data.TransactionEvent.CMLData.InstanceID"), logDF("data.TransactionEvent.CMLData.SessionLogCounter"), logDF("data.TransactionEvent.TransactionStart")).show

    val txn_event_df1 = txn_event_df.select(parseDateTime(col("timestamp")).alias("APPLIANCE_TS_UTC").cast("timestamp"), col("json_signature").alias("EPHEMERAL_HMAC"),
      col("data.TransactionEvent.ApplianceVersion").alias("APPLIANCE_VERSION"), col("data.TransactionEvent.TransactionEventID").alias("TransactionID"),
      col("data.TransactionEvent.ClientIP").alias("ClientIP"), col("data.TransactionEvent.CMLData.CMLVersion").alias("CMLVersion"), col("data.TransactionEvent.CMLData.CMLDataVer").alias("CMLDataVer"),
      col("data.TransactionEvent.CMLData.InstanceID").alias("InstanceID"), col("data.TransactionEvent.CMLData.InstanceLogCounter").alias("InstanceLogCounter"),
      col("data.TransactionEvent.CMLData.SessionID").alias("SessionID"), col("data.TransactionEvent.CMLData.SessionLogCounter").alias("SessionLogCounter"),
      //col("data.TransactionEvent.CMLData.UserData").alias("UserData"), col("data.TransactionEvent.CMLData.UserData.qXCoord").alias("Q_X_COORD"),
      //col("data.TransactionEvent.CMLData.UserData.qYCoord").alias("Q_Y_COORD"), col("data.TransactionEvent.CMLData.UserData.qWaferId").alias("Q_WAFER_ID"),
      //col("data.TransactionEvent.CMLData.UserData.qCMLibVer").alias("Q_CM_LIBVER"),
      col("data.TransactionEvent.TransactionStart").alias("TransactionStart").cast("long"),
      col("data.TransactionEvent.TransactionEnd").alias("TransactionEnd").cast("long"), col("data.TransactionEvent.RequestedModuleName").alias("RequestedModuleName"),
      col("data.TransactionEvent.ModuleVersion").alias("ModuleVersion").cast("long"), col("data.TransactionEvent.DeviceID").alias("DeviceID"), col("data.TransactionEvent.Result.value").alias("RESULT_VALUe"),
      col("data.TransactionEvent.Result.resultCode").alias("RESULT_CODE"), col("data.TransactionEvent.HSMLog.encoding").alias("HSMLOG_ENCODING"), col("data.TransactionEvent.HSMLog.value").alias("HSMLOG_VALUE"),
      col("data.TransactionEvent.HSMLogData.Chip Series").alias("HSMLOG_CHIP_SERIES"), //col("data.TransactionEvent.HSMLogData.Debug Unlock Mask").alias("HSMLOG_DEBUG_UNLOCK_MASK"),
      //col("data.TransactionEvent.HSMLogData.Debug Unlock Value").alias("HSMLOG_DEBUG_UNLOCK_VALUE"), col("data.TransactionEvent.HSMLogData.volatile_debug_unlock").alias("HSMLOG_VOLATILE_DEBUG_UNLOCK"),
      col("data.TransactionEvent.HSMLogData.Device Id").alias("HSMLOG_DEVICE_ID"), col("data.TransactionEvent.HSMLogData.Provisioning Authority Device AES Key Id").alias("HSMLOG_PROV_AUTH_DVC_KEY_ID"),
      col("data.TransactionEvent.HSMLogData.Module Name").alias("HSMLOG_MODULE_NAME"), col("data.TransactionEvent.HSMLogData.Module Version").alias("HSMLOG_MODULE_VERSION")).withColumn("SOURCE_LOG_FILE", lit(file.getName)).
      withColumn("SOURCE_LOG_ENV", lit(source_log_evn)).withColumn("CLUSTER_FOLDER_NAME", lit(cluster_folder_name)).withColumn("BATCH_ID", lit(batch_id)).withColumn("CLUSTER_ID", lit(cluster_id)).withColumn("TXN_LOG_SEQ", lit(txn_seqnum)).
      withColumn("QUARANTINE_LOG", lit("No"))

    txn_event_df1.printSchema()
    txn_event_df1.show()

    txn_event_df1.registerTempTable("temp")

    hiveContext.sql("create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")

    //val final_df = hiveContext.sql("SELECT txn_log_seq + row_sequence() as TXN_LOG_SEQNUM, cast(FROM_UNIXTIME(APPLIANCE_TS) as timestamp) as APPLIANCE_TS_UTC, * from temp")
    //final_df.drop(col("txn_log_seq")).drop(col("APPLIANCE_TS")).printSchema()
    //final_df.drop(col("TXN_LOG_SEQ")).drop(col("APPLIANCE_TS")).write.mode("append").saveAsTable("cri_transaction_event_log")


    val final_df = hiveContext.sql("SELECT txn_log_seq + row_sequence() as TXN_LOG_SEQNUM, * from temp")

    final_df.drop(col("txn_log_seq")).printSchema()

    final_df.drop(col("TXN_LOG_SEQ")).write.mode("append").saveAsTable("cri_transaction_event_log")


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
