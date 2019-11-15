package com.tjh.sparkstudy.streaming

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object HubbleTask {
  def sendKafkaMsg(msg_id: String, msg: String, task_id: String, zk: String): Unit = {
    var msgMap = new JSONObject()
    msgMap.put("msg_type", "notice")
    msgMap.put("task_id", task_id)
    msgMap.put("msg_id", msg_id)
    msgMap.put("msg_info", msg)
    val properties = new Properties()
    properties.put("metadata.broker.list", zk)
    //  0：这意味着生产者producer不等待来自broker同步完成的确认继续发送下一条（批）消息。此选项提供最低的延迟但最弱的耐久性保证（当服务器发生故障时某些数据会丢失，如leader已死，但producer并不知情，发出去的信息broker就收不到）。
    //  1：这意味着producer在leader已成功收到的数据并得到确认后发送下一条message。此选项提供了更好的耐久性为客户等待服务器确认请求成功（被写入死亡leader但尚未复制将失去了唯一的消息）。
    //  -1：这意味着producer在follower副本确认接收到数据后才算一次发送完成。
    //  此选项提供最好的耐久性，我们保证没有信息将丢失，只要至少一个同步副本保持存活。
    //  三种机制，性能依次递减 (producer吞吐量降低)，数据健壮性则依次递增
    //  properties.put( "request.required.acks","1" )
    //由于我们发送的是字符串 默认是按照byte数组发送的 这里需要明确指定编码格式
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("fetch.message.max.bytes", "100000000")
    properties.put("max.partition.fetch.bytes", "100000000")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("compression.codec", "snappy")

    //可以给定具体的分区器
    //    properties.put("partitioner.class", "com.qsq.rc.demo.ProducerPartitioner")
    //构建producer上下文
    val producerConfig = new ProducerConfig(properties)
    val producer = new Producer[String, String](producerConfig)
    //val jsonMsg:String=JSON.toJSONString(msgMap)
    producer.send(new KeyedMessage[String, String]("dw_hubble_compute_data", msgMap.toJSONString))
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("hubble_task").master("yarn").enableHiveSupport().getOrCreate()
    val batchTime = Seconds(3)
    val streamContext = new StreamingContext(sparkSession.sparkContext, batchTime)
    //这个路径放hdfs上
    streamContext.checkpoint("/user/tangjuhong/hubble_checkout_tmp")
    val aryTops = new util.ArrayList[String]()
    aryTops.add("dw_hubble_compute_data")
    val kafkaParam = new util.HashMap[String, Object]()
    val brokers = "10.105.205.29:9092,10.105.211.78:9092,10.105.240.148:9092,10.105.251.238:9092,10.105.87.170:9092"
    kafkaParam.put("metadata.broker.list", brokers)
    kafkaParam.put("bootstrap.servers", brokers)
    kafkaParam.put("group.id", "hubble_rule_consumer")
    kafkaParam.put("max.partition.fetch.bytes", "50000000")
    kafkaParam.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(aryTops, kafkaParam)
    )
    var task_id = ""
    println("see data")
    messages.foreachRDD { record => {
      var mysqlJdbc = new MysqlJdbc();
      try {
        println("traverse data")
        val rdd = record.map(_.value())
        //解析json，拿到sql和task_id
        val jsonOBJ: JSONObject = JSON.parseObject(rdd.toString())
        val task_id: String = jsonOBJ.getString("task_id")
        val execute_sql = jsonOBJ.getString("sql")
        val msg_type = jsonOBJ.getString("msg_type")
        if (msg_type.toLowerCase.contains("compute")) {
          val call_sql = "update hubble_rule set solve_status=1 where keyid=" + task_id
          //更新为处理中
          mysqlJdbc.execute(call_sql)
          val dataFrame: DataFrame = sparkSession.sql(execute_sql)
          dataFrame.show(10)
          var write_mysql_tablename = "hubble_data_" + task_id.toInt % 5
          //单条插入
          dataFrame.foreach(row => {
            val user_id = row.getAs[String]("user_id")
            val toMysql =
              s"""
                 |INSERT INTO ${write_mysql_tablename}
                 |(user_id,task_id,addtime)
                 |VALUES
                 |(${user_id},${task_id},NOW())
               """.stripMargin
            mysqlJdbc.execute(toMysql)
          })

          mysqlJdbc.execute("update hubble_rule set solve_status=3 where keyid=" + task_id)
          ///推送kafka消息
          sendKafkaMsg("1000", "数据处理完成,请速查询", task_id, brokers)
        }
      } catch {
        case e: Exception =>
          println("solve exception:" + e.printStackTrace())
          mysqlJdbc.execute("update hubble_rule set solve_status=2 where keyid=" + task_id)
          sendKafkaMsg("9999", "数据处理发生异常:" + e.printStackTrace(), task_id, brokers)
      }
      finally {
        mysqlJdbc.close()
      }
    }

      streamContext.start() //spark stream系统启动
      streamContext.awaitTermination()
      println("solve over")
    }
  }
}
