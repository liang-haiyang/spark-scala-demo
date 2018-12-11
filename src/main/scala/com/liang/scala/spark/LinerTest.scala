package com.liang.scala.spark

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
 * @author  lianghaiyang 2018/12/11 9:44
 *  spark学习之--->线性回归
 */
object LinerTest {
  def test(): Unit = {
    val spark = SparkSession.builder().appName("App").master("local").getOrCreate()
    val data_path = "data/linear_data.txt"
    val training = spark.read.format("libsvm").load(data_path)
    val lr = new LinearRegression()
      .setMaxIter(1000)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lrModel = lr.fit(training)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.predictions.show()
  }
}
