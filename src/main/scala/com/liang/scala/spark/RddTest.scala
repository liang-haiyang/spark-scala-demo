package com.liang.scala.spark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author lianghaiyang 2018/12/11 9:46
  *         spark学习之RDD
  */
object RddTest {
  def rddTest(): Unit = {
    val data_path = "data/rdd_data.txt"
    val ss = SparkSession.builder().master("local").getOrCreate()
    val sc = ss.sparkContext
    val data = sc.textFile(data_path)
    val training = data.map { line =>
      val arr = line.split(',')
      LabeledPoint(arr(0).toDouble, Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.cache()
    training.foreach(println)
    sc.stop()
  }

  def rddTest1(): Unit = {
    val data_path = "data/rdd_data1.txt"
    val ss = SparkSession.builder().master("local").getOrCreate()
    val sc = ss.sparkContext
    //读取文件 生成RDD
    val file: RDD[String] = sc.textFile(data_path)
    val word: RDD[String] = file.flatMap(_.split(" "))
    //    RDD元组
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    val wordCount = wordOne.reduceByKey(_ + _)
    wordCount.saveAsTextFile("F:\\WorkSpace\\idea\\test\\scala\\scalademo\\444.txt")
    print(s"wordCount: ${wordCount.collect()}")
    val sortedRDD = wordCount.sortBy(tuple => tuple._2, ascending = false)
    print(s"sortedRDD: ${sortedRDD.collect()}")
    sc.stop()
  }
}
