package com.cub.demo02_unbounded.demo05_sink.sample02_redis

import java.net.InetSocketAddress

import com.cub.Raytek
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisConfigBase}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
  * Description：Redis Sink案例演示   Source:socket，Sink:redis  <br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月02日  
  *
  * @author
  * @version : 1.0
  */
object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②读取源，进行处理，最后落地到redis中
    //a)计算结果
    val result: DataStream[Raytek] = env.socketTextStream("47.104.86.109", 9999)
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

    //b)落地到redis中
    //i)准备连接redis分布式集群的参数信息
    val nodes: Set[InetSocketAddress] = Set(
      new InetSocketAddress("47.104.86.109", 7001),
      new InetSocketAddress("47.104.86.109", 7002),
      new InetSocketAddress("NODE02", 7003),
      new InetSocketAddress("NODE02", 7004),
      new InetSocketAddress("NODE03", 7005),
      new InetSocketAddress("NODE03", 7006)
    )

    //ii)将scala中的Set集合转换成Java中的Set集合
    import scala.collection.JavaConversions._

    //iii)构建FlinkJedisConfigBase实例
    val flinkJedisConfigBase: FlinkJedisConfigBase = new FlinkJedisClusterConfig.Builder()
      .setNodes(nodes)
      .build()

    val redisSinkMapper: RedisMapper[Raytek] = new MyRedisMapper()

    result.addSink(new RedisSink(flinkJedisConfigBase, redisSinkMapper))

    //③启动
    env.execute(this.getClass.getSimpleName)
  }


  /**
    * 准备RedisMapper特质的实现类
    */
  class MyRedisMapper extends RedisMapper[Raytek] {
    /**
      * 用来定制保存数据的类型,一个DataStram下述方法只会执行一次
      *
      * @return
      */
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "allTempExceptionTravllers")
    }

    /**
      * 指定key
      *
      * @param data
      * @return
      */
    override def getKeyFromData(data: Raytek): String = {
      data.id + data.name
    }

    /**
      * 指定value
      *
      * @param data
      * @return
      */
    override def getValueFromData(data: Raytek): String = {
      data.toString
    }
  }

}
