package com.cub.demo02_unbounded.demo03_source.sample03_kafka

import java.util.Properties

//import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.scala._
//import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}

/**
  * Description：从kafka分布式集群中实时采集数据案例演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author
  * @version : 1.0
  */
object ReadDataFromKafkaDemo extends App {
  //步骤：
  //①环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //②从kafka中获得数据，计算，并显示
  val topic = "raytek"
  val valueDeserializer: DeserializationSchema[String] = new SimpleStringSchema
  val props: Properties = new Properties
  //将资源目录下的配置文件装载到Properties实例中，封装了和kafka服务器连接的参数信息
  props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
  env.addSource(new FlinkKafkaConsumer[String](topic, valueDeserializer, props))
    .print("kafka→")

  //③启动
  env.execute(this.getClass.getSimpleName)
}

/*
1.要添加 flink 与 kafka 整合的依赖包

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>${scope}</scope>
        </dependency>


2. 关于如何导入隐式类
import org.apache.flink.api.scala._

a. double shift --> scala --> class
b. 点击 类名 Copy Reference  即 org.apache.flink.api.scala
c. 前加import 后加._      import xx._

 */