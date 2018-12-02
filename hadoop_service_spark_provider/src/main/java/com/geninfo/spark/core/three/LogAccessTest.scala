package com.geninfo.spark.core.three

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


case class AddClick(time: String,
                    province: String,
                    city: String,
                    uid: String,
                    adId: String)

object LogAccessTest {

  /**
    * 数据格式: 时间 省份 城市 用户 广告
    * 统计每一个省份点击top3的广告ID
    */
  def xuqiuq_1(addClickRDD: RDD[AddClick]): Unit = {
    // 将数据转换为k-v结构，将数据力度转换为  省份中的广告
    val province_add_count: RDD[(String, Int)] = addClickRDD.map(addClick => (addClick.province + "___" + addClick.adId, 1))

    // 计算每一省份每一广告的总点击量
    val privince_add_sum: RDD[(String, Int)] = province_add_count.reduceByKey(_ + _)

    // 计算数据粒度转换为省份
    val province_ad: RDD[(String, (String, Int))] = privince_add_sum.sortBy(_._2, false).map {
      case (province__adId, count) => {
        val param = province__adId.split("___")
        (param(0).toString, (param(1).toString, count))
      }
    }
    val province_ads: RDD[(String, Iterable[(String, Int)])] = province_ad.groupByKey()

    //得出结果
    val result: RDD[String] = province_ads.flatMap { case (province, items) =>

      // 当前省份的TOP3的广告集合
      val filterItems: Array[(String, Int)] = items.toList.sortWith(_._2 < _._2).take(3).toArray
      filterItems.foreach(print)

      val result = new ArrayBuffer[String]()

      for (item <- filterItems) {
        result += (province + "、" + item._1 + "、" + item._2)
      }
      result
    }
    result.collect().foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("log").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //加载数据
    val logRDD: RDD[String] = sc.textFile("F:\\WORKSPACE\\big_datas\\dd.txt")

    //    // 将数据转换为对象
    val addClickRDD: RDD[AddClick] = logRDD.map { x =>
      val param = x.split(",")
      AddClick(param(0).toString, param(1).toString, param(2).toString, param(3).toString, param(4).toString)
    }
    xuqiuq_1(addClickRDD)
    sc.stop()
  }
}
