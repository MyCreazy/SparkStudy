package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created with IntelliJ IDEA.
  * User: tangjuhong
  * Date: 2018/9/5
  * Time: 下午3:35
  **/
object SparkSql {
  /**
    * main函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //    {"id":1, "name":"leo", "age":18}
    //    {"id":2, "name":"jack", "age":19}
    //    {"id":3, "name":"marry", "age":17}
    val conf = new SparkConf().setMaster("local").setAppName("sparksql");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    ////数据源为json字符串
    //    val jsonrdd= sqlContext.read.json("/Applications/xx.txt")
    ////如果不用从本地读取json字符串
    val nameRDD = sc.makeRDD(Array(
      "{\"name\":\"zhangsan\",\"age\":18,\"id\":3}"
    ))

    val jsonrdd=sqlContext.read.json(nameRDD)
    ////引入一个隐式转换
    import sqlContext.implicits._
    val studentdf = jsonrdd.toDF()
    ////打印表结构
    studentdf.show()
    ////注册一个表
    studentdf.registerTempTable("t_student")
    val df = sqlContext.sql("select *from t_student where id=3")
    df.rdd.foreach(row => println("结果" + row(0) + "  " + row(1) + "  " + row(2)))
    println("sparksql解析完成")
  }
}

case class Student(id: Int, name: String, age: Int)
