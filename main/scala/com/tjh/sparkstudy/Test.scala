package com.tjh.sparkstudy

import org.apache.spark.sql.SparkSession

/**
  * Created by tjh.
  * Date: 2019/7/3 下午5:02
  **/
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test").master("local[1]")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    )) toDF("name", "age", "phone")

    df.show()
    System.out.print("success")
  }
}
