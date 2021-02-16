package com.cub.demo02_unbounded.demo05_sink.sample03_es

import com.cub.Raytek
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions._

/**
  * Description：es sink演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月02日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ESSinkDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②读取源，进行处理，最后落地到es中
    import org.apache.flink.api.scala._
    //a)计算结果
    val result: DataStream[Raytek] = env.socketTextStream("NODE01", 9999)
      .filter(_.trim.nonEmpty)
      .map(perLine => {
        val arr = perLine.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).filter(perTraveller => perTraveller.temperature < 36.3 || perTraveller.temperature > 37.2) //←筛选出体温异常的旅客信息


    //b)落地到es中存储起来

    //i)准备httpHosts，用于存储与远程es分布式集群连接的信息
    val httpHosts: List[HttpHost] = List(
      new HttpHost("NODE01", 9200),
      new HttpHost("NODE02", 9200),
      new HttpHost("NODE03", 9200)
    )


    //ii)准备ElasticsearchSinkFunction特质实现类的实例，用于操作es分布式集群
    val elasticsearchSinkFunction: ElasticsearchSinkFunction[Raytek] = new MyESSinkFunction()

    ///iii)构建Builder的实例
    val builder: ElasticsearchSink.Builder[Raytek] =
      new ElasticsearchSink.Builder[Raytek](httpHosts, elasticsearchSinkFunction)

    //iiii)ElasticsearchSink为每个批次请求设置要缓冲的最大操作数（document条数）
    builder.setBulkFlushMaxActions(1)

    //iiiii) 正式构建ElasticsearchSink实例
    val esSink: ElasticsearchSink[Raytek] = builder.build()

    //iiiiii)sink到es
    result.addSink(esSink)


    //③启动
    env.execute(this.getClass.getSimpleName)
  }

  /**
    * 自定义ElasticsearchSinkFunction特质实现类，用来向es中存入计算后的数据
    */
  class MyESSinkFunction extends ElasticsearchSinkFunction[Raytek] {
    /**
      *
      * 当前的DataStream中每流动一个元素，下述的方法就触发执行一次
      *
      * @param element
      * @param ctx
      * @param indexer
      */
    override def process(element: Raytek, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

      print(s"→待处理的体温异常的旅客信息是：$element ")

      //i)将当前的Raytek实例中的信息封装到Map中
      //id: String, temperature: Double, name: String, timestamp: Long, location: Strin
      val scalaMap = Map[String, String](
        "id" -> element.id.trim,
        "temperature" -> element.temperature.toString.trim,
        "name" -> element.name.trim,
        "timestamp" -> element.timestamp.toString.trim,
        "location" -> element.location.trim
      )

      val javaMap: java.util.Map[String, String] = scalaMap

      //ii)构建indexRequest实例

      val indexRequest = Requests.indexRequest()
        .index("raytek")
        .`type`("traveller")
        .id(s"${element.id.trim}→${element.name.trim}")
        .source(javaMap)

      //iii)存储
      indexer.add(indexRequest)
    }
  }

}

