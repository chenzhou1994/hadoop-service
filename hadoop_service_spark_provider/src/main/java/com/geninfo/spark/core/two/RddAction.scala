package com.geninfo.spark.core.two

import org.apache.spark.{SparkConf, SparkContext}


object RddAction extends App {

  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]");
  val sc = new SparkContext(sparkConf);

  val array = 1 to 20;
  val arrayTwo = 1 to 10;
  val text = Array("hello world", "hello chen zhou", "hello chen ")

  // 1. reduce
  // println(sc.makeRDD(array).reduce(_ + _))

  // 2. collect 将RDD的数据返回到driver层进行输出
  // 3. count  计算RDD的数据数量
  // 4. first 返回RDD的第一个元素
  // 5. take(n) 返回RDD的前n个元素
  // 6. takeSample()采样
  // 7. takeOrdered 返回排序后的前几个数据

  // 8. aggregate
  // 9. fold
  // 10. saveAsTextFile(path)
  // 11. saveAsSequenceFile(path)
  // 12. saveAsObjectFile(path)
  // 13. countByKey 返回每个key的数据量
  // 14. forEach
}
