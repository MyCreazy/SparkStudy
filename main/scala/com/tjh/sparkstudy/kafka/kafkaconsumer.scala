package kafka



import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.JavaConversions._



/**
  * Created with IntelliJ IDEA.
  * User: tangjuhong
  * Date: 2018/9/5
  * Time: 下午3:13
  **/
object kafkaconsumer{

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "10.1.0.4:9092,10.1.0.9:9092,10.1.0.2:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    props.put("auto.offset.reset","earliest")
    ////自动周期确认消息
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("logs"))
    while (true){
      val records = consumer.poll(100)
      for (record <- records){
        println(record.offset() +"--" +record.key() +"--" +record.value())
        println("1")
      }

      println("2")
    }
    consumer.close()

  }
}
