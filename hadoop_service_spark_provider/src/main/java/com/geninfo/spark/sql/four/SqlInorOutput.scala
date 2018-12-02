package com.geninfo.spark.sql.four

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SqlInorOutput extends App {
  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  val sc = spark.sparkContext

  // 输入(高级)
  val employee = spark.read.json("F:\\WORKSPACE\\big_datas\\example\\employees.json")
  // 输入(低级)
  // spark.read.format("json").load("F:\\WORKSPACE\\big_datas\\example\\employees.json")

  // 输出(低级)
  //  employee.write.format("jdbc")
  //    .option("driver", "com.mysql.jdbc.Driver")
  //    .option("url", "jdbc:mysql://47.98.236.124:3306/bigdata")
  //    .option("dbtable", "rdd01").option("user", "root").option("password", "123456")
  //    .mode("overwrite").save()

  val employees = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver")
    .option("url", "jdbc:mysql://47.98.236.124:3306/bigdata")
    .option("dbtable", "rdd01").option("user", "root").option("password", "123456").load()
  employees.show()
  spark.close()
}
