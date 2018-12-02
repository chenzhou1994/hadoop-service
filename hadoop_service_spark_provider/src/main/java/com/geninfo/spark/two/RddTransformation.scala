package com.geninfo.spark.two

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddTransformation extends App {
  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]");
  val sc = new SparkContext(sparkConf);

  val array = 1 to 20;
  val arrayTwo = 1 to 10;
  val text = Array("hello world", "hello chen zhou", "hello chen ")

  // 1. map 一对一转换 [[1,2],[3]]
  // sc.makeRDD(array).map(item => item * 2).collect().foreach(println)

  // 2. filter 过滤数据
  // sc.makeRDD(array).filter(item => item % 2 != 0).collect().foreach(println)

  // 3. flatMap 一对多，并将多压平 [1,2,3]
  // sc.makeRDD(array).flatMap(item => 1 to item).collect().foreach(print)

  // 4. mapPartitions 对于每个分区执行一次该函数，传入的是一个分区的数据->参数是Iterator
  // sc.makeRDD(array).mapPartitions(items => items.filter(_ % 3 == 0).map(_ + " hello")).collect().foreach(println)

  // 5. mapPartitionsWithIndex 同上,不过加了分区索引
  // sc.makeRDD(array, 5).mapPartitionsWithIndex((index, items) => items.map("所在分区索引->" + index + ", 数据->" + _)).collect().foreach(println)

  // 6. sample 主要用于抽样
  // sc.makeRDD(array).sample(true, 0.4, 1).collect().foreach(println)

  // 7. union 组合
  //sc.makeRDD(array).union(sc.makeRDD(arrayTwo)).collect().foreach(println)

  // 8. intersection 交集
  // sc.makeRDD(array).intersection(sc.makeRDD(arrayTwo)).collect().foreach(println)

  // 9. distinct 去重
  // sc.makeRDD(array).union(sc.makeRDD(arrayTwo)).distinct().collect().foreach(println)

  // 10. partitionBy 自定义分区
  // sc.makeRDD(array).map(item => (item, item)).partitionBy(new org.apache.spark.HashPartitioner(5)).mapPartitionsWithIndex((index, items) => items.map("所在分区索引->" + index + ", 数据->" + _)).collect().foreach(println)

  // 11. reduceByKey 根据KEY进行聚合
  // sc.makeRDD(text).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

  // 12 groupByKey(): RDD[(K, Iterable[V])]  根据KEY进行聚合，预聚合
  // sc.makeRDD(Array((1, 1), (1, 2), (2, 3), (2, 4), (3, 5))).groupByKey().collect().foreach(println)

  /**
    * 13. combineByKey[C](
    * createCombiner: V => C,
    * mergeValue: (C, V) => C,
    * mergeCombiners: (C, C) => C,
    * numPartitions: Int): RDD[(K, C)]
    */
  //  sc.makeRDD(Array(("a", 88), ("a", 79), ("b", 82), ("b", 81), ("b", 73))).combineByKey(
  //    score => (score, 1),
  //    (firstComScore: (Int, Int), nextScore) => (firstComScore._1 + nextScore, firstComScore._2 + 1),
  //    (parSumScores: (Int, Int), otherParSumScores: (Int, Int)) => (parSumScores._1 + otherParSumScores._1, parSumScores._2 + otherParSumScores._2))
  //    .map { case (name, sumScores: (Int, Int)) => (name, sumScores._1 / sumScores._2) }
  //    .collect().foreach(println)

  /**
    * 14. aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
    * combOp: (U, U) => U): RDD[(K, U)]
    */
  //  sc.makeRDD(Array(("a", 88), ("a", 79), ("b", 82), ("b", 81), ("b", 73))).aggregateByKey(0)(Math.max(_, _), _ + _).collect().foreach(println)
  //  sc.makeRDD(Array(("a", 88), ("a", 79), ("b", 82), ("b", 81), ("b", 73))).combineByKey(
  //    score => Math.max(0, score),
  //    (firstComScore: Int, v) => Math.max(firstComScore, v),
  //    (c1: Int, c2: Int) => Math.max(c1, c2)).collect().foreach(println)

  // 15. foldByKey 是aggregateByKey的简化版
  //  sc.makeRDD(Array(("a", 88), ("a", 79), ("b", 82), ("b", 81), ("b", 73))).foldByKey(0)(_ + _)

  // 16. sortByKey 如果key不支持排序，需要with Ordering接口
  // 17. sortBy 根据传入的函数进行排序
  // sc.makeRDD(array).sortBy(item => item % 2, true).collect().foreach(println)

  //18. join、leftoutjoin.. 连接两个RDD数据
  //sc.makeRDD(Array((1, "a"), (2, "b"), (2, "c"))).join(sc.makeRDD(Array((1, "我"), (2, "dw"), (3, "erg")))).collect().foreach(println)

  // 19. cogroup
  // 20. cartesian 笛卡儿积
  // 21. pipe 执行外部脚本
  // 22. coalesce
  // 23. repartition 重新分区
  // 24. glom
  // 25. mapValues
  // 26. subtract
  sc.stop()
}
