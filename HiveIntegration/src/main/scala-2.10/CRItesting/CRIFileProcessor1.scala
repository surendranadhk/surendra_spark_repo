package CRItesting

import java.io.File

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by hduser on 11/15/16.
  */
object CRIFileProcessor1 {

  def main(args: Array[String]): Unit = {

    val f = new File("/home/hduser/CRI_Converstion/d27f92fe-6bfa-4ee9-a734-145f9022a776-2231.manifest")

    val lines = scala.io.Source.fromFile(f).getLines().toList
    
    val conf = new SparkConf().setMaster("local").setAppName("log analysis")
    val sc = new SparkContext(conf)

    System.setProperty("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    val hiveContext = new HiveContext(sc)

    val max_df = hiveContext.sql("select max(manifest_seqnum) from cri_manifest1")
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
      val cm_log = CRIManifestFile(cm.manifest_seqnum, cm.batch_id, cm.cluster_id, eachLog._2, eachLog._1, cm.transaction_log_count, cm.client_log_count, f.getName)
      l += cm_log
    }

    val rdd = sc.parallelize(l)
    val df = hiveContext.createDataFrame(rdd)
    df.registerTempTable("temp")

    val cri_df = hiveContext.sql("select *, cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as timestamp) as ETL_LOAD_TS_UTC from temp")
    //cri_df.printSchema()
    //cri_df.show()
    cri_df.write.mode("append").saveAsTable("cri_manifest1")

  }
}
