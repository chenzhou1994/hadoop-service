package com.geninfo.spark.sql.three

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

// 强类型的UDAF函数
class MySalaryAvery extends Aggregator[Employee, Average, Double] {
  // 初始化每个分区中的共享变量
  override def zero: Average = Average(0L, 0)

  // 每个分区中的每一条数据聚合时需要调用的方法
  override def reduce(b: Average, a: Employee): Average = {
    b.sum = b.sum + a.salary
    b.count = b.count + 1
    b
  }

  // 将每个分区的输出合并成最后的数据
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 输出计算结果
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 主要用于对共享变量进行编码
  override def bufferEncoder: Encoder[Average] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object MySalaryAvery {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("as").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val employee = spark.read.json("G:\\big_datas\\example\\employees.json").as[Employee];
    employee.createOrReplaceTempView("employee")
    val ave = new MySalaryAvery().toColumn.name("average")
    employee.select(ave).show()
    spark.stop()
  }
}
