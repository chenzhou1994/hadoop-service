package com.geninfo.spark.core.cdn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex


object CdnStatics {
  val logger = LoggerFactory.getLogger(CdnStatics.getClass)

  //匹配IP地址
  val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小
  val httpSizePattern = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  //统计每个IP的访问量前10位
  def ipStatics(data: RDD[String]): Unit = {
    val ipRDD: RDD[(String, Int)] = data.map(item => (IPPattern.findFirstIn(item).get, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    ipRDD.take(10).foreach(item => {
      println("ip: " + item._1 + ", 访问次数: " + item._2)
    })
  }

  // 统计每个视频被访问的独立IP数
  def videoIpStatics(data: RDD[String]): Unit = {
    def getFileNameAndIp(line: String): (String, String) = {
      (videoPattern.findFirstIn(line).toString, IPPattern.findFirstIn(line).toString)
    }

    val videoRDD: RDD[(String, List[String])] = data.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).
      map(item => getFileNameAndIp(item)).groupByKey().map(item => (item._1, item._2.toList.distinct))
      .sortBy(_._2.size, false)

    videoRDD.take(10).foreach(item => println("视频: " + item._1 + ",被访问次数: " + item._2.size))
  }


  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {

    def isMatch(pattern: Regex, str: String) = {
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }

    /**
      * 获取日志中小时和http 请求体大小
      *
      * @param line
      * @return
      */
    def getTimeAndSize(line: String) = {
      var res = ("", 0L)
      try {
        val httpSizePattern(code, size) = line
        val timePattern(year, hour) = line
        res = (hour, size.toLong)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
      res
    }

    //3.统计一天中每个小时间的流量
    data.filter(x => isMatch(httpSizePattern, x)).filter(x => isMatch(timePattern, x)).map(x => getTimeAndSize(x)).groupByKey()
      .map(x => (x._1, x._2.sum)).sortByKey().foreach(x => println(x._1 + "时 CDN流量=" + x._2 / (1024 * 1024 * 1024) + "G"))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CdnStatics")
    val sc = new SparkContext(conf)

    // IP 命中率 响应时间 请求时间 请求方法 请求URL 请求协议 状态码 响应大小 referer  用户代理
    val textFile = sc.textFile("F:\\WORKSPACE\\workspace\\my_space\\spark\\spark_core\\spark_wc\\src\\main\\resources\\cdn.txt").cache()

    // IP访问量前10位
    ipStatics(textFile)
    videoIpStatics(textFile)
    flowOfHour(textFile)
  }
}
