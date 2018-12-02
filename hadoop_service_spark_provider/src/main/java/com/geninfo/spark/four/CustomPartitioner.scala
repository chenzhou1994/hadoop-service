package com.geninfo.spark.four

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomPartitioner(numPartition: Int) extends Partitioner {
  // 分区的总数
  override def numPartitions: Int = {
    numPartition
  }

  // 根据传入的key返回分区的索引
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numPartition
  }
}

object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]");
    val sc = new SparkContext(sparkConf);

    val array = Array(1 to 100);
    val rdd = sc.makeRDD(array,1).zipWithIndex()
    // 键值型RDD
    rdd.mapPartitionsWithIndex((index, items) => Iterator(index + ":【" + items.mkString(",") + "】")).collect().foreach(println)
    val rdd2 = rdd.partitionBy(new CustomPartitioner(5))

    rdd2.mapPartitionsWithIndex((index, items) => Iterator(index + ":【" + items.mkString(",") + "】")).collect().foreach(println)

  }

}
