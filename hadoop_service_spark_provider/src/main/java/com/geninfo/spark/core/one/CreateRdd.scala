package com.geninfo.spark.core.one

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.从集合中创建RDD
  * 2.从外部存储创建RDD
  * 3.从其他RDD转换
  */
object CreateRdd extends App {

  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]");
  val sc = new SparkContext(sparkConf);

  val array = Array(1, 2, 3, 4, 5);

  // 1.从集合中创建RDD
  //  sc.makeRDD(array).collect().foreach(println)
  //  sc.parallelize(array).collect().foreach(println)


  sc.stop()
}
