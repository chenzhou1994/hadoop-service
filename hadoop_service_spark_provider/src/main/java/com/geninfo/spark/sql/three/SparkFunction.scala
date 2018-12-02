package com.geninfo.spark.sql.three

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 弱类型UDAF函数,小聚合，大聚合
  */
class SalaryAvery extends UserDefinedAggregateFunction {

  // 输入数据
  override def inputSchema: StructType = StructType(StructField("salary", LongType) :: Nil)

  // 每一个分区中的共享变量
  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", IntegerType) :: Nil)

  // 表示UDAF的输出类型
  override def dataType: DataType = DoubleType

  // 表示有相同的输入是否会存在相同的输出，如果是为true
  override def deterministic: Boolean = true

  // 初始化每个分区中的共享变量
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // sum
    buffer(0) = 0L
    //count
    buffer(1) = 1
  }

  //每个分区中的每一条数据聚合时，调用该方法
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    println("====>>" + input.getLong(0) + ",===>" + buffer.getLong(0) + ",=====>" + buffer.getInt(1))
    //获取这一行的工资，然后加入到sum
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    // 将工资数+1
    buffer(1) = buffer.getInt(1) + 1
  }

  // 将每一个分区的数据合并，形成最终的数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 合并总工资
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

    // 合并总的公子个数
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // 给出计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }
}

case class Score(name: String, Clazz: Int, score: Int)

object SparkUdf {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("as").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val employee = spark.read.json("F:\\WORKSPACE\\big_datas\\example\\employees.json");
    val scoreDF = spark.sparkContext.makeRDD(Array(
      Score("a", 1, 89),
      Score("b", 1, 79),
      Score("c", 1, 94),
      Score("d", 2, 89),
      Score("e", 2, 74),
      Score("f", 3, 92),
      Score("g", 3, 83),
      Score("h", 3, 99),
      Score("i", 3, 89),
      Score("g", 3, 81))).toDF("name", "class", "score")

    employee.createOrReplaceTempView("employee")
    // 1. UDF函数注册
    spark.udf.register("add", (x: String) => "NAME:" + x)
    spark.sql("select employee.*,add(name) from employee").show()

    // 2. 弱类型UDAF函数注册
    spark.udf.register("average", new SalaryAvery)
    spark.sql("select average(salary) from employee").show()

    //3. 开窗函数
    scoreDF.createOrReplaceTempView("score")
    spark.sql("select s.*,rank() over(partition by class order by score desc) rank from score s").show()

    spark.stop()
  }
}
