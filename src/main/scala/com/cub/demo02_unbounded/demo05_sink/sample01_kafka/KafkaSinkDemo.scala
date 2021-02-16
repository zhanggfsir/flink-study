package com.cub.demo02_unbounded.demo05_sink.sample01_kafka

import java.util.Properties

import com.cub.Raytek
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._

/**
  * Description：kafkaSink演示     Source:kafka ，Sink:kafka <br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日  
  *
  * @author
  * @version : 1.0
  */
object KafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //②从kafka中源主题中实时读取流数据，并进行后续的迭代计算，筛选出体温异常的旅客信息
    //String topic, DeserializationSchema<T> valueDeserializer, Properties props

    val props = new Properties
    props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    val exceptionDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("raytekSrc", new SimpleStringSchema, props))
      .map(perMsg => {
        val arr = perMsg.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).filter(
      perTraveller => {
      val temp = perTraveller.temperature
      val normal = temp >= 36.3 && temp <= 37.2
      !normal
    }).map(_.toString)


    //③将体温异常的旅客信息保存到kafka目标主题中
    exceptionDataStream.addSink(
      new FlinkKafkaProducer[String]("NODE01:9092,NODE02:9092,NODE03:9092", "raytekTarget", new SimpleStringSchema)
    )

    //④启动
    env.execute

  }
}
