package com.geninfo.spark.sql.two

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class people(name: String, age: Int)

object DataTransformation extends App {
  // 声明配置 一般用local模式就可以
  val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  val peopledata = sc.textFile("F:\\WORKSPACE\\big_datas\\example\\people.txt")

  //1 RDD ==>>DataFrame
  //1.1
  val peopleDataFrame = peopledata.map { item =>
    val para = item.split(",")
    (para(0), para(1).trim.toInt)
  }.toDF("name", "age")
  peopleDataFrame.show()

  //1.2 利用反射
  val peopleRdd2Frame = peopledata.map { x =>
    val para = x.split(",")
    people(para(0), para(1).trim.toInt)
  }.toDF()
  peopleRdd2Frame.show()

  //1.3 编程实现
  val schemaString = "name age"
  val fiields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fiields)
  val rowRdd = peopledata.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
  val peopleDF = spark.createDataFrame(rowRdd, schema)
  peopleDF.show()

  //2. DataFrame ==>> RDD
  val dataFrame2Rdd = peopleDataFrame.rdd
  // dataFrame2Rdd.collect().foreach(println)
  // dataFrame2Rdd.map(_.getString(0)).collect().foreach(println)

  //3. RDD ==>> DataSet (case class确定schema)
  val peopleSet: Dataset[people] = peopledata.map { item =>
    val para = item.split(",")
    people(para(0), para(1).trim.toInt)
  }.toDS()
  peopleSet.show()


  //4. DataSet ==>> RDD
  val dataSetRDD: RDD[people] = peopleSet.rdd
  dataSetRDD.map(_.name).collect().foreach(println)

  //5. DataSet ==>> DataFrame
  val peopleSet2DataFrame = peopleSet.toDF()
  peopleSet2DataFrame.show()

  //6. DataFrame ==>> DataSet (case class确定schema)
  val peopleFrame2DataSet = peopleSet2DataFrame.as[people]
  peopleFrame2DataSet.show()
  spark.stop()

}
