package org.training.spark.streaming.extend

import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Extra functions added to the API
 */
class ExtraFunctions[T:ClassTag](dstream:DStream[T]) {

  def fold(zeroValue: T)(op: (T, T) => T):DStream[T]  = {
    dstream.transform(rdd => rdd.context.makeRDD(List(rdd.fold(zeroValue)(op))))
  }

}

object ExtraFunctions {

  implicit def addExtraFunctions[T:ClassTag](dStream: DStream[T]) = new ExtraFunctions[T](dStream)
}
