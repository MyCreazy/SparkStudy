package com.tjh.sparkstudy.streaming

import kafka.consumer.ConsumerConfig
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tjh.
  * Date: 2018/12/6 上午10:24
  **/
object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    ////目前这个版本不支持receiver模式，只支持从kafka直接拉数据
    val sparkConf = new SparkConf().setAppName("kafkastreamming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    //max.poll.records表示多少条就处理一次
    val kafkaParams = Map(
      "bootstrap.servers"->"127.0.0.1:9092",
      "group.id" -> "test",
      "auto.offset.reset" -> "earliest",
      "max.poll.records"->"2",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"
    )

    //val topics = Set("test")
    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaParams))

    print("直接读取完成")
    kafkaTopicDS.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        val sortData = data.sortBy(_._2, false)
        sortData
      })
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
