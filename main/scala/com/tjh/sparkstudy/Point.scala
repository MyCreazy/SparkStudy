package com.test

import java.io.{FileNotFoundException, IOException}

class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    try {
      x = x + dx
      y = y + dy
      println("x 的坐标点: " + x);
      println("y 的坐标点: " + y);
    }
    catch {
      case ex: FileNotFoundException => {
        println("发生异常文件没有找到")
      }

      case ex: IOException => {
        println("IO 发生异常")
      }
    }
    finally {
      println("移动完成")
    }
  }
}


