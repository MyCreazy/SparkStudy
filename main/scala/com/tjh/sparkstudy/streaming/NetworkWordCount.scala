package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tjh.
  * Date: 2018/12/5 下午7:10
  **/
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    ////这里这个local一定要设置数字，如果只是local只会接收数据，不会展示处理结果
    val sparkConf = new SparkConf().setAppName("streamingtest").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1));
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordcounts=words.map(x => (x, 1)).reduceByKey(_ + _)
    print("数目："+wordcounts.count())
    wordcounts.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
