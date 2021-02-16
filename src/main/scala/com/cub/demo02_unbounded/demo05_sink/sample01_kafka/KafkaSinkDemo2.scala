package com.cub.demo02_unbounded.demo05_sink.sample01_kafka

import java.lang
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.cub.Raytek
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Description：kafkaSink演示（优化版）   Source:kafka ，Sink:kafka  <br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日  
  *
  * @author
  * @version : 1.0
  */
object KafkaSinkDemo2 {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(2000)


    //②从kafka中源主题中实时读取流数据，并进行后续的迭代计算，筛选出体温异常的旅客信息
    val props = new Properties
    props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    val exceptionDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("raytekSrc", new SimpleStringSchema, props))
      .filter(_.nonEmpty)//处理非空的情形
      .map(perMsg => {
        val arr = perMsg.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).filter(perTraveller => {
      val temp = perTraveller.temperature
      val normal = temp >= 36.3 && temp <= 37.2
      !normal
    }).map(_.toString)  // 转string


    //③将体温异常的旅客信息保存到kafka目标主题中
    val defaultTopic = "raytekTarget"
    val serializationSchema: KafkaSerializationSchema[String] = new KafkaSerializationSchema[String]() {
      /**
        * 针对于DataStream中的每一个元素，都会调用下述的方法进行序列化后，发布到kafka消息队列中存储起来
        *
        * @param element 消息的value, 红外测温仪测到的体温偏高的旅客信息
        * @param timestamp
        * @return
        */
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](defaultTopic, element.getBytes(StandardCharsets.UTF_8))
      }
    }
    val producerConfig = new Properties()
    producerConfig.load(this.getClass.getClassLoader.getResourceAsStream("kafka/producer2.properties"))
    val semantic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE



    exceptionDataStream.addSink(
      new FlinkKafkaProducer[String](defaultTopic, serializationSchema, producerConfig, semantic)
    )
//    exceptionDataStream.addSink(
//      new FlinkKafkaProducer[String]("NODE01:9092,NODE02:9092,NODE03:9092", "raytekTarget", new SimpleStringSchema)
//    )
    //④启动
    env.execute

  }
}
