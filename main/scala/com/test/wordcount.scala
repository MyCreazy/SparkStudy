package com.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 词频统计
  */
object wordcount {
  /**
    * 没有继承Application,实现main函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
        ////由于没有通过外部传参数，所以这里未判断入参
        System.out.println("start....")
        val conf = new SparkConf().setMaster("local").setAppName("counttest")
        val sc = new SparkContext(conf);
        val lines = sc.textFile("/Applications/xx.txt")
        val words = lines.flatMap(line => line.split(" "))
        val pairs = words.map(word => (word, 1))
        val wordcount = pairs.reduceByKey(_ + _)
        wordcount.foreach(wordcount => println(wordcount._1 + "  " + wordcount._2))
        sc.stop();
        System.out.println("static word over")
  }
}
