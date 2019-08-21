package com.tjh.sparkstudy.clean

import java.util.Calendar

import java.util.Calendar

import com.tjh.sparkstudy.hbase.HbaseOpt
import org.apache.spark.sql.SparkSession

/**
  * Created by tjh.
  * Date: 2018/12/28 下午5:42
  **/
object ACardGrade {
  def main(args: Array[String]): Unit = {
    print("start runing")
    val sparkSession = SparkSession.builder().master("yarn").appName("ACard").enableHiveSupport().getOrCreate()
    var sql = "select *from tdm.tdm_s_grade_card_base";
    var acardbaseData = sparkSession.sql(sql)
    acardbaseData.show(10)
    ////数据质量不行，住房情况，学历,年收入都拿不到
    acardbaseData.foreach(row => {
      try {
        val bornDate = row.getAs("ocr_birth").toString
        val ageArray = bornDate.split("-")
        var age = 0
        if (ageArray.length > 1) {
          val year = ageArray(0).toInt
          val cal = Calendar.getInstance
          val currentYear = cal.get(Calendar.YEAR)
          age = currentYear - year
        }
        var ageGrade = 0
        if (age >= 18 || age <= 22) {
          ageGrade = 2
        } else if (age >= 23 || age <= 34) {
          /////这个岁数范围需要看一下比如是否读书等，这里先取一个默认值
          ageGrade = 7
        }
        else if (age >= 35 || age <= 40) {
          ageGrade = 15
        }
        else if (age >= 41 || age <= 60) {
          ageGrade = 7
        }
        else if (age >= 61) {
          ageGrade = 3
        }

        var genderStr = row.getAs("individual_gender").toString
        var genderGrade = 0
        if ("f".equals(genderStr.toLowerCase())) {
          ////女
          genderGrade = 4
        }
        else if (("m").equals(genderStr.toLowerCase())) {
          genderGrade = 2
        }

        var marryStr = row.getAs("individual_extend_marry_status").toString
        var marryGrade = 0
        if (("true").equals(marryStr.toLowerCase())) {
          marryGrade = 12
        } else {
          marryGrade = 8
        }

        var educationStr = row.getAs("individual_extend_education").toString
        var educationGrade = 0
        if (("true").equals(educationStr.toLowerCase())) {
          educationGrade = 10
        }
        else {
          educationGrade = 2
        }

        var rentStr = row.getAs("individual_extend_house_type").toString
        var rentGrade = 0
        if (("rent").equals(rentStr.toLowerCase())) {
          rentGrade = 9
        }
        else {
          rentGrade = 5
        }
        var workTimeGrade = 1

        var workTypeStr = row.getAs("individual_job_occupation").toString
        var workTypeGrade = 0
        if ("1".equals(workTypeStr)) {
          workTypeGrade = 10
        }
        else if ("2".equals(workTypeStr)) {
          workTypeGrade = 6
        }
        else if ("3".equals(workTypeStr)) {
          workTypeGrade = 4
        }
        else {
          workTypeGrade = 1
        }

        var annualIncomeGrade = 1
        var sumGrade = annualIncomeGrade + workTimeGrade + rentGrade + educationGrade + marryGrade + genderGrade + ageGrade + workTypeGrade
        var identity = row.getAs("individual_identity").toString
        var userName = row.getAs("individual_name").toString
        var phoneNumber = row.getAs("individual_card_mobile").toString
        ////数据写入hbase
        HbaseOpt.insertTable(identity, "cf", "userName", userName)
        HbaseOpt.insertTable(identity, "cf", "identity", identity)
        HbaseOpt.insertTable(identity, "cf", "phoneNumber", phoneNumber)
        HbaseOpt.insertTable(identity, "cf", "ageGrade", ageGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "genderGrade", genderGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "marryGrade", marryGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "educationGrade", educationGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "rentGrade", rentGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "workTypeGrade", workTypeGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "workTimeGrade", workTimeGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "annualIncomeGrade", annualIncomeGrade.toString)
        HbaseOpt.insertTable(identity, "cf", "sumGrade", sumGrade.toString)
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
    )

    sparkSession.close()
  }

}
