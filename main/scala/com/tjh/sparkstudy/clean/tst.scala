package com.tjh.sparkstudy.clean

import java.util.Calendar

/**
  * Created by tjh.
  * Date: 2018/12/29 下午7:12
  **/
object tst {
 /* val bornDate = row.getAs("ocr_birth").toString
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

  var annualIncome = 1
  var sumGrade = annualIncome + workTimeGrade + rentGrade + educationGrade + marryGrade + genderGrade + ageGrade
  var hivesql = "insert into tdm.tdm_s_acard partition(dt='20180101')" +
    "select " + oauth_user_id + "," + individual_name + "," + individual_gender + "," + individual_identity + "," + sumGrade
  print(hivesql)*/
}
