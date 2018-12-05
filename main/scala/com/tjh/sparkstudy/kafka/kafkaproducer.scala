package kafka

import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created with IntelliJ IDEA.
  * User: tangjuhong
  * Date: 2018/9/5
  * Time: 下午3:57
  **/
object kafkaproducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list","10.1.0.4:9092,10.1.0.9:9092,10.1.0.2:9092")  // broker 如果有多个,中间使用逗号分隔
    props.put("serializer.class","kafka.serializer.StringEncoder")
    props.put("request.required.acks","1")
    val config = new ProducerConfig(props)
    val producer = new Producer[String,String](config)
    val runtime = new Date().toString
    var msg = "message publishing time - " + runtime
    val topic="logs" //注意topic
    for(i<- 1 to 1000)
    {
      msg="send msg:"+i
      val data = new KeyedMessage[String, String](topic, msg)
      producer.send(data)
      println(msg+" success")
    }
    producer.close()
  }
}
