package com.geninfo.spark.sql.two

import com.jeninfo.two.DataTransformation.peopledata
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExecMode extends App {
  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  val peopledata = sc.textFile("F:\\WORKSPACE\\big_datas\\example\\people.txt")

  val peopleDataFrame = peopledata.map { item =>
    val para = item.split(",")
    (para(0), para(1).trim.toInt)
  }.toDF("name", "age")

  //1.DSL模式,通过调用方法
  //  peopleDataFrame.select("name").show()
  //  peopleDataFrame.filter($"age" > 25).show()

  //2.SQL模式
  // session内可访问，一个session结束后，表自动删除
  peopleDataFrame.createOrReplaceTempView("people")
  // 应用级别内可访问，使用表明加上"global"前缀
  peopleDataFrame.createGlobalTempView("people")

  spark.sql("select * from people").show()
  spark.sql("select * from global_temp.people").show()

}
