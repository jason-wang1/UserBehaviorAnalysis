package com.wang.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月21日
  *
  * @author WangBo
  * @version 1.0
  */
object KafkaProducer {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个Kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // 从文件中读取数据发送
    val bufferedSource: BufferedSource = io.Source.fromFile("C:\\Users\\BoWANG\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}
