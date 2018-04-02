package hivetesting

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hduser on 11/7/16.
  */
object HiveIntegration {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark-Hive Integration").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer.mb","64")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val results1 = sqlContext.sql("SELECT * FROM cri_transaction_log_sample")

    results1.write.format("com.databricks.spark.csv").option("header", "true").save(args(0))

  }

}
