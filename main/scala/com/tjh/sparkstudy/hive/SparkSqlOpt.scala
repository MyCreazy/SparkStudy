package com.tjh.sparkstudy.hive

import hive.md5udaf.md5logic
import org.apache.spark.sql.SparkSession

/**
  * Created by tjh.
  * Date: 2019/8/21 下午2:53
  **/
object SparkSqlOpt {
  def main(args: Array[String]): Unit = {
    val jdbc = new Jdbc()
    var id = 0
    var iserror = false
    var msg = ""
    try {
      val comarisonName = "tjhtest"
      val comparisons = jdbc.query(s"select * from comparison_record where comparison_name='$comarisonName' order by start_time desc limit 1")
      id = Integer.valueOf(comparisons.get(0).get("id"))
      jdbc.execute(s"UPDATE comparison_record SET run_status=3 where id=$id")
      val spark = SparkSession.builder().master("local[1]").appName("test").enableHiveSupport().getOrCreate()
      spark.sql("select *From ods.xxx")
      println("test")
    }
    catch {
      case e: Exception => {
        println(e.getMessage)
        iserror = true
        msg = e.getMessage.replaceAll("'","")
        jdbc.execute(s"UPDATE comparison_record SET run_status=2,error_msg='$msg' where id=$id")

      }
    }
    finally {

    }


  }
}
