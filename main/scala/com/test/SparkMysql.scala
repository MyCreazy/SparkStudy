package com.test

import java.io.IOException
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object SparkMysql {
  def main(args: Array[String]): Unit = {
    ////查询操作
    this.query()

  }

  /**
    * 插入操作
    */
  def createTable(): Unit = {
  val sparksession=SparkSession.builder().master("local").appName("createtalbe").getOrCreate()
    sparksession.sparkContext.setLogLevel("WARN")

  }

  /**
    * mysql查询操作
    */
  def query(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("mysql");
    val sc = new SparkContext(conf);
    try {
      val sqlContext = new SQLContext(sc);
      val proerties = new Properties();
      proerties.put("user", "root")
      proerties.put("password", "root")
      val url = "jdbc:mysql://127.0.0.1:3306/studentinfo?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull"
      ////这里设置查询条件，以及需要查询出的字段
      val student_df = sqlContext.read.jdbc(url, "test", Array("id=1", "id=2"), proerties).select("id", "age", "name")

      student_df.show()
      ////然后已注册领时表的方式进行操作
      student_df.registerTempTable("mtest")
      ////如果是操作多个表，操作类似，这里可以用两种方式操作，一种是sqlContext，另一种是使用DF
      var result = sqlContext.sql("select * from mtest where id=1")
      result.rdd.foreach(row => println("SqlContext方式：" + row(0) + " " + row(1) + " " + row(2)))
      student_df.select("id", "name").where("id=1").show()
    }
    catch {
      case ex: IOException => {
        println("IO异常")
      }
    }
    finally {
      sc.stop()
    }
    println("sql统计完成")
  }
}
