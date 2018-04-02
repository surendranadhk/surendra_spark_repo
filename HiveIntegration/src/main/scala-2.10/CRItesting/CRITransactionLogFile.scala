package org.training.spark.testing

import scala.collection.mutable
import scala.runtime.ScalaRunTime

/**
  * Created by sumantht on 11/17/2016.
  */
class CRITransactionLogFile(var txnlog_seqnum: Long, var client_ip: String, var transaction_id: String, var appliance_id: String, var result_value: String,
                                 var result_code: Int, var transaction_start_org: Long, var transaction_end_org: Long, var cml_version: String,
                                 var cml_data_ver: Long, var instance_id: Long, var instance_log_counter:Long, var session_id: Long,
                                 var session_log_counter: Long, var user_data: String, var q_x_coord: Int, var q_y_coord: Int, var q_wafer_id: Long,
                                 var requested_module_name: String, var module_version: Long, var hsmlog_encoding: String, var hsmlog_value: String,
                                 var hsmmac_encoding: String, var hsmmac_value: String, var hsmlog_chip_series: String, var hsmlog_device_id: String,
                                 var hsmlog_module_name: String, var hsmlog_module_version: String, var hsmlog_prov_auth_dvc_key_id: String, var hsmlog_serial_cvdakey_padakey: String,
                                 var hsmlog_hdcp_ksv: String, var hsmlog_debug_unlock_mask: String, var hsmlog_debug_unlock_value: String, var hsmlog_volatile_debug_unlock: String,
                                 var batch_id: String, var cluster_id: String, var source_log_file: String, var source_log_evn: String, var cluster_folder_name: String,
                                 var device_id: String, var appliance_ts_utc: String, var ephemeral_hmac : String, var product: String, var appliance_version: String) extends Product with Serializable {

  /*override def toString: String = {
      val fields = (mutable.LinkedHashMap[String, Any]() /: this.getClass.getDeclaredFields) { (a, f) =>
        f.setAccessible(true)
        a += (f.getName -> f.get(this))
        a
      }
      s"${this.getClass.getSimpleName}(${fields.values.mkString(",")})"
  }*/

  override def canEqual(that: Any) = that.isInstanceOf[CRITransactionLogFile]

  override def equals(that: Any) = ScalaRunTime._equals(this, that)

  override def hashCode() = ScalaRunTime._hashCode(this)

  override def toString = ScalaRunTime._toString(this)

  override def productPrefix = classOf[CRITransactionLogFile].getSimpleName

  override def productArity = this.getClass.getDeclaredFields.length

  def productElement(n: Int): Any = { //n match {
    /*case 0 => this.txnlog_seqnum
    case 1 => this.client_ip
    case _ => throw new IndexOutOfBoundsException(n.toString)*/
    var i = -1
    val fields = (mutable.LinkedHashMap[Int, Any]() /: this.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      i = i + 1
      a += (i -> f.get(this))
      a
    }
    fields.get(n).getOrElse("Unknown")
  }

}
