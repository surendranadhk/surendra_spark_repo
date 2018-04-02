package CRItesting

/**
  * Created by hduser on 11/15/16.
  */
case class CRIManifestFile(var manifest_seqnum: Long, var batch_id: String, var cluster_id: String,
                           var appliance_id: String, var filename: String, var transaction_log_count: Long, var client_log_count: Long, var mainfest_filename: String)