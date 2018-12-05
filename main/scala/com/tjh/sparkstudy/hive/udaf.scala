package hive

import org.apache.spark.sql.{Row, SparkSession}
import org.dmg.pmml.DefineFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

/**
  * Created with IntelliJ IDEA.
  * User: tangjuhong
  * Date: 2018/9/12
  * Time: 下午5:30
  **/
object udaf {

  object  MyAverage extends  UserDefinedAggregateFunction()
  {
    /**
      * 指定输入的数据类型
      * 字段名称随意
      * @return
      */
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    /**
      * 在进行聚合操作的时候所要处理的数据的中间结果类型
      * @return
      */
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    /**
      * 返回类型
      * @return
      */
    def dataType: DataType = DoubleType

    /**
      * 是否设置同样的输入返回同样的输出
      * @return
      */
    def deterministic: Boolean = true

    /**
      *  设置聚合中间buffer的初始值，但需要保证这个语义：两个初始buffer调用下面实现的merge方法后也应该为初始buffer。
      *  即如果你初始值是1，然后你merge是执行一个相加的动作，两个初始buffer合并之后等于2，不会等于初始buffer了
      * @param buffer
      */
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    /**
      * 用输入数据input更新buffer值,类似于combineByKey
      * @param buffer
      * @param input
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    /**
      * //合并两个buffer,将buffer2合并到buffer1.在合并两个分区聚合结果的时候会被用到,类似于reduceByKey
      *  这里要注意该方法没有返回值，在实现的时候是把buffer2合并到buffer1中去，你需要实现这个合并细节
      * @param buffer1
      * @param buffer2
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    /**
      * 计算并返回最终的聚合结果
      * @param buffer
      * @return
      */
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble /buffer.getLong(1).toDouble
  }

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[1]").appName("UDAFTest").getOrCreate()
    spark.udf.register("myAverage",MyAverage)
    val json= spark.read.json("/Users/tangjuhong/kuainiu/sourcecode/SparkStudy/main/resources/employees.json")
    json.createOrReplaceTempView("employees")
    json.show();
    spark.sql("select  myAverage(salary) from employees").show()
    println("Query Success")

  }

}
