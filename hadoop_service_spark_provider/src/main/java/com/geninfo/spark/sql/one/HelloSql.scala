package com.geninfo.spark.sql.one

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloSql extends App {
  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sc = spark.sparkContext

  val employee = spark.read.json("F:\\WORKSPACE\\big_datas\\example\\employees.json")
  employee.show()
  employee.printSchema()
  // employee.map(_.getAs[String]("name"))
  employee.select("name").show()

  employee.createOrReplaceTempView("employee")
  spark.sql("select * from employee").show()
}
