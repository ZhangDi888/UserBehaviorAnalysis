import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object testKafka {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    //key和value的序列化
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    //常见一个kafka producer
    val producer = new KafkaProducer[String,String](properties)
    //从文件读取测试数据，逐条发送
    val source = io.Source.fromFile("D:\\ideaworkspace\\Flink_UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //遍历这个源
    for (line <- source.getLines()) {
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }
}
