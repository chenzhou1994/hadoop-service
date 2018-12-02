package com.geninfo.spark.five

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object InorOutput extends App {
  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]");
  val sc = new SparkContext(sparkConf);
  val data = Array(("张三", 34, "1"))
  //1. 文本文件的输入输出
  // val rdd = sc.textFile("")
  // rdd.saveAsTextFile()

  //2. json文件的输入输出，需要在程序中进行编解码

  //3. csv(逗号分隔)、tsv(tab分割)文件的输入输出

  //4. sequenceFile文件的输入输出，读取需要设定kv类型
  // sc.sequenceFile("")

  //5. 对象文件的输入输出,读取需要设定kv类型
  // val rdd: RDD[(Int, String)] = sc.objectFile("")
  // rdd.saveAsObjectFile()

  //6. hadoop的输入输出

  //7. 关系型数据库的输入输出
  val staffRdd = new JdbcRDD(sc, () => {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    java.sql.DriverManager.getConnection("jdbc:mysql://47.98.236.124:3306/bigdata", "root", "123456")
  }, "select * from staff where id >= ? and id <= ?;", 1, 3, 1, r => (r.getInt(1), r.getString(2)))

  staffRdd.collect().foreach(println)

  def inseartData(itertor: Iterator[(String, Int, String)]): Unit = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://47.98.236.124:3306/bigdata", "root", "123456")
    itertor.foreach(data => {
      val ps = conn.prepareStatement("insert into staff(name,age,sex) value (?,?,?);")
      ps.setString(1, data._1)
      ps.setInt(2, data._2)
      ps.setString(3, data._3)
      ps.executeLargeUpdate()
    })
  }

  sc.makeRDD(data).foreachPartition(inseartData)
}
