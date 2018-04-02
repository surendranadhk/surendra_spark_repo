package org.training.spark.mllib.recommendations.movierecommendation

import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.recommendation.ALS
//import org.apache.spark.ml.recommendation.ALS.Rating

import org.apache.spark.mllib.recommendation.{ALS, Rating}
//import org.jblas.DoubleMatrix


/**
 * Created by hduser on 1/8/16.
 */



object  MovieRecom {


  /* Compute Mean Average Precision at K */

  /* Function to compute average precision given a set of actual and predicted ratings */
  // Code for this function is based on: https://github.com/benhamner/Metrics
  /*def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }

  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }*/

  def main(args: Array[String]) {

    /* Load the raw ratings data from a file. */
    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "MovieRecommendation", sparkConf)
    val rawData = sc.textFile(args(1))
    rawData.first()


    /* Extract the user id, movie id and rating only from the dataset */
    val rawRatings = rawData.map(_.split("\t").take(3))

    /* Construct the RDD of Rating objects */
    val ratings = rawRatings.map {
      case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toFloat)
    }
    ratings.first()



    /* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
    /* rank: This refers to the number of factors in our ALS model, that is,
    the number of hidden features in our low-rank approximation matrices.
      Generally, the greater the number of factors, the better, but this has a
    direct impact on memory usage, both for computation and to store models
    for serving, particularly for large number of users or items. Hence, this is
      often a trade-off in real-world use cases. A rank in the range of 10 to 200 is
      usually reasonable.
    • iterations: This refers to the number of iterations to run. While each
      iteration in ALS is guaranteed to decrease the reconstruction error of the
      ratings matrix, ALS models will converge to a reasonably good solution after
      relatively few iterations. So, we don't need to run for too many iterations in
      most cases (around 10 is often a good default).
    • lambda: This parameter controls the regularization of our model.
    Thus, lambda controls over fitting. The higher the value of lambda,
    the more is the regularization applied. What constitutes a sensible value
    is very dependent on the size, nature, and sparsity of the underlying data,
    and as with almost all machine learning models, the regularization parameter
    is something that should be tuned using out-of-sample test data and
      cross-validation approaches.
    */
    val model = ALS.train(ratings, 50, 10, 0.01)
    //val model = ALS.train(ratings, rank = 50)



    /* Inspect the user factors */
    model.userFeatures


    /* Count user factors and force computation */
    model.userFeatures.count


    model.productFeatures.count


    /* Make a prediction for a single user and movie pair */

    val samplepredictedRating = model.predict(789, 123)

    /* Make predictions for a single user across all movies */
    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))




    /* Load movie titles to inspect the recommendations */
    val movies = sc.textFile(args(2))
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)


    val moviesForUser = ratings.keyBy(_.user).lookup(789)


    println(moviesForUser.size)
    // 33
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)

    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)


    /* Compute item-to-item similarities between an item and the other items */

    //val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    /*

    val itemId = 567
    val x  = model.productFeatures.lookup(itemId)
    x
    val itemFactor = model.productFeatures.lookup(itemId).head

    val itemVector = new DoubleMatrix(itemFactor)


    //cosineSimilarity(itemVector, itemVector)

    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }

    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })


    println(sortedSims.mkString("\n"))





    /* We can check the movie title of our chosen movie and the most similar movies to it */
    println(titles(itemId))

    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim) }.mkString("\n")




    /* Compute squared error between a predicted and actual rating */
    // We'll take the first rating for our example user 789
    val actualRating = moviesForUser.take(1)(0)


    val predictedRating = model.predict(789, actualRating.product)

    val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)


    /* Compute Mean Squared Error across the dataset */
    // Below code is taken from the Apache Spark MLlib guide at: http://spark.apache.org/docs/latest/mllib-guide.html#collaborative-filtering-1
    val usersProducts = ratings.map{ case Rating(user, product, rating)  => (user, product)}
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    val ratingsAndPredictions = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)

    val MSE = ratingsAndPredictions.map{
      case ((user, product), (actual, predicted)) =>  math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)


    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)



    val actualMovies = moviesForUser.map(_.product)
    // actualMovies: Seq[Int] = ArrayBuffer(1012, 127, 475, 93, 1161, 286, 293, 9, 50, 294, 181, 1, 1008, 508, 284, 1017, 137, 111, 742, 248, 249, 1007, 591, 150, 276, 151, 129, 100, 741, 288, 762, 628, 124)
    val predictedMovies = topKRecs.map(_.product)
    // predictedMovies: Array[Int] = Array(27, 497, 633, 827, 602, 849, 401, 584, 1035, 1014)
    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
    // apk10: Double = 0.0

    apk10

    /* Compute recommendations for all users */
    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)
    // (1682,50)

    // broadcast the item factor matrix
    val imBroadcast = sc.broadcast(itemMatrix)

    // compute recommendations for each user, and sort them in order of score so that the actual input
    // for the APK computation will be correct
    val allRecs = model.userFeatures.map{ case (userId, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq
      (userId, recommendedIds)
    }

    // next get all the movie ids per user, grouped by user id
    val userMovies = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)


    // finally, compute the APK for each user, and average them to find MAPK

    val MAPK = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, K)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)


    /* Using MLlib built-in metrics */

    // MSE, RMSE and MAE
    import org.apache.spark.mllib.evaluation.RegressionMetrics
    val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)


    // MAPK
    import org.apache.spark.mllib.evaluation.RankingMetrics
    val predictedAndTrueForRanking = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)


    // Compare to our implementation, using K = 2000 to approximate the overall MAP
    val MAPK2000 = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)
    // Mean Average Precision = 0.07171412913757186
	
	*/
  }

}











