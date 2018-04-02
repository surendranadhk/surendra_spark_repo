package org.training.spark.mllib.lr.predict

import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 18/9/16.
 */
object HomePriceRecommender {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster(args(0)).setAppName("Home Price Recommender"))
    val linRegModel = sc.objectFile[LinearRegressionModel]("file:////home/hduser/Projects/spark-machinelearning/LRModels/homePricing-model").first()
    val scalerModel = sc.objectFile[StandardScalerModel]("file:///home/hduser/Projects/spark-machinelearning/LRModels/homePricingScaler-model").first()

    // home.age, home.bathrooms, home.bedrooms, home.garage, home.sqF
    println(linRegModel.predict(scalerModel.transform(Vectors.dense(11.0, 2.0, 2.0, 1.0, 2200.0))))
    sc.stop()
  }

}
