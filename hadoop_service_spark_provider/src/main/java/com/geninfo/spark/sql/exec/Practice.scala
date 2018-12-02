package com.geninfo.spark.sql.exec

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

object Practice {

  private def insertMySQL(tableName: String, dataDF: DataFrame): Unit = {
    dataDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://47.98.236.124:3306/bigdata")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "123456")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    // 加载数据到Hive
    val tbStockRdd = spark.sparkContext.textFile("F:\\WORKSPACE\\workspace\\my_space\\spark\\spark_sql\\sparksqlhelloworld\\src\\main\\resources\\tbStock.txt")
    val tbStockDS = tbStockRdd.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS
    tbStockDS.createOrReplaceTempView("tbStock")

    val tbStockDetailRdd = spark.sparkContext.textFile("F:\\WORKSPACE\\workspace\\my_space\\spark\\spark_sql\\sparksqlhelloworld\\src\\main\\resources\\tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr => tbStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    val tbDateRdd = spark.sparkContext.textFile("F:\\WORKSPACE\\workspace\\my_space\\spark\\spark_sql\\sparksqlhelloworld\\src\\main\\resources\\tbDate.txt")
    val tbDateDS = tbDateRdd.map(_.split(",")).map(attr => tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS
    tbDateDS.createOrReplaceTempView("tbDate")

    //需求一： 统计所有订单中每年的销售单数、销售总额
    //    val result1 = spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear")
    //    insertMySQL("xq1", result1)
    //insertMySQL("tbStock", tbStockDS.toDF())
    //insertMySQL("tbStockDetail", tbStockDetailDS.toDF())
    insertMySQL("tbDate", tbDateDS.toDF())
  }
}
