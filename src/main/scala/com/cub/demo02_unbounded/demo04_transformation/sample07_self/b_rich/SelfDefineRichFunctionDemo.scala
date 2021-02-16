package com.cub.demo02_unbounded.demo04_transformation.sample07_self.b_rich

import java.util.Properties

import com.cub.Raytek
import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Description：用户自定义富函数，需求→一批旅客正在通过各个关口设置的红外测温仪，
  * 将体温异常的旅客信息单独提取出来，存入到kafka消息队列中；将体温正常的旅客不予干预，直接通过监测出口。<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日
  *
  * @author
  * @version : 1.0
  */
object SelfDefineRichFunctionDemo {
  /*
  实现的步骤：
①启动kafka分布式集群，新建一个名为temperature_exception的主题
②编写自定义的富函数，如：MyRichFlatMapFuntion
a）close →进行资源的释放
b）open →进行初始化的操作
c) flatMap →进行业务处理,将当前的旅客信息根据指定的业务进行处理
  i)体温正常的，直接送往主DataStrem进行后续的处理
  ii)体温异常，将该旅客的信息存入到kafka消息队列特定的主题分区中
③验证
nc -lk 777
   */
  def main(args: Array[String]): Unit = {
    //前提：
    //①拦截非法的参数
    if (args == null || args.length != 4) {
      sys.error(
        """
          |请传入参数！--hostname <主机名> --port <端口号>
        """.stripMargin)
      sys.exit(-1)
    }

    //②获得参数值
    val tool = ParameterTool.fromArgs(args)
    val hostname = tool.get("hostname")
    val port = tool.getInt("port")

    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    //主题名
    val topic: String = "temperatureException"

    //封装与kafka分布式集群连接的一些参数
    val properties: Properties = new Properties
    properties.load(this.getClass.getClassLoader.getResourceAsStream("kafka/producer.properties"))



    env.socketTextStream(hostname, port)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).flatMap(new MyRichFlatMapFuntion(topic, properties)) //map增强 RichMapFunction ...
      .print("经由富函数处理，体温正常的旅客信息是→")

    //③启动
    env.execute
  }

  /**
    *
    * @param topic      主题名
    * @param properties 该参数值中封装了与kafka分布式集群建立连接的一系列的参数
    */
  class MyRichFlatMapFuntion(topic: String, properties: Properties) extends RichFlatMapFunction[Raytek, Raytek] {

    /**
      * 准备全局变量producer，给后续的方法flatMap来使用。 val 常量[此处会报错]   var 变量
      */
    private var producer: KafkaProducer[String, String] = _


    /**
      * 进行初始化的工作，分析当前的DataStream时也只会执行一次
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      producer = new KafkaProducer[String, String](properties)
    }


    /**
      * 在该方法中要书写业务
      *
      * 下述的方法会执行n次，从DataStream中每次流过来一条信息，就会触发执行一次
      *
      * @param value 封装了DataStream中实时产生的一条旅客信息
      * @param out   将处理后的数据收集到新的DataStream中
      */
    override def flatMap(value: Raytek, out: Collector[Raytek]): Unit = {
      val normal: Boolean = value.temperature >= 36.3 && value.temperature <= 37.2

      if (normal) {
        //  i)体温正常的，直接送往主DataStrem进行后续的处理
        out.collect(value)
      } else {
        //  ii)体温异常，将该旅客的信息存入到kafka消息队列特定的主题分区中
        val msg: ProducerRecord[String, String] = new ProducerRecord(topic, value.toString)
        producer.send(msg)
      }

    }


    /**
      * 进行资源释放的
      *
      * 下述方法针对于当前的DataStream只会执行一次
      */
    override def close(): Unit = {
      if (producer != null)
        producer.close
    }
  }


}
