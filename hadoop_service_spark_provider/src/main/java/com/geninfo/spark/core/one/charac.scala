package com.geninfo.spark.core.one

import org.apache.spark.{SparkConf, SparkContext}

object charac extends App {
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val str = "宽7口径1kkj6kn5kjb4k1老年1健康12不健2康吧3，lfi9ihn3kj2"
  sc.makeRDD(str.toList).filter(item => item.isDigit).map((_, 1)).reduceByKey(_ + _).collect().foreach(x => println(x._1 + " 出现了:" + x._2 + "次"))
  print(str.toList.filter(_.isDigit).size)
}
