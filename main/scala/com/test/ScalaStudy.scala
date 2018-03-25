package com.test


object ScalaStudy extends App {

  println(matchTest())

  def matchTest(x:Any):String=x match {
    case 1=>"one"
    case "xukong"=>"my name is xukong"
    case "2"=>"two"
    case _ =>"我是默认值"
  }

  ////迭代器
  //  val it = Iterator("dd", "33", "xxx")
  //  while (it.hasNext) {
  //    println(it.next())
  //  }


  ////数组遍历
  //  var arrr = Array(2, 3, 4)
  //  for (x <- arrr) {
  //    println(x)
  //  }


  ////闭包学习
  //    var factor = 3
  //    val multiplier = (i: Int) => i * factor
  //    System.out.print(multiplier(2))
}
