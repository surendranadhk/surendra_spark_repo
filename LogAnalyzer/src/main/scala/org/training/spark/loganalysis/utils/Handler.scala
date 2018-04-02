package org.training.spark.loganalysis.utils

import scala.collection.mutable.ArrayBuffer

/**
 * Handler for all external write systems
 * @author hduser
 */
trait Handler {
  /**
   * Closes the stream
   */
  def close()
}
