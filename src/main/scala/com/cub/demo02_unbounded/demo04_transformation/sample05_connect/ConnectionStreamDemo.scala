package com.cub.demo02_unbounded.demo04_transformation.sample05_connect

import com.cub.Raytek
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

/**
  * Description：使用DataStream的算子之connect将两个类型不同的流合并在一起，分别记性单独处理。<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ConnectionStreamDemo {
  /*
① DataStream,DataStream→ConnectedStreams：连接两个保持他们类型的数据流，两个数据流被
Connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
② 连接流可以对不同类型的DataStream合并后进行集中式地处理
   */
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    val splitStream: SplitStream[Raytek] = env.socketTextStream("47.104.86.109", 7777)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      })
      .split((rayteck: Raytek) => if (rayteck.temperature >= 36.3 && rayteck.temperature <= 37.2) Seq("正常") else Seq("异常"))

    //针对不同特征的旅客，进行不同的处理
    //a)从流中取出所有体温正常的旅客信息
    val normalStream: DataStream[(String, String)] = splitStream.select("正常")
      .map(perEle => (perEle.id, s"名字为【${perEle.name}】的旅客体温正常哦！... "))

    //b)从流中取出所有体温异常的旅客信息，进行处理（显示）
    val exceptionStream: DataStream[(String, String, String)] = splitStream.select("异常")
      .map(perEle => (perEle.id, perEle.name, s"该旅客体温偏高，需要隔离... "))

    //将上述两种类型的流合并起来，进行集中式地分析处理，然后输出
    val connectedStream: ConnectedStreams[(String, String), (String, String, String)] = normalStream.connect(exceptionStream)

    connectedStream.map(
      normal => ("红外测温仪的id→" + normal._1, "旅客的信息是→" + normal._2),
      exception => ("红外测温仪的id→" + exception._1, "旅客的名字是→" + exception._2, "警报信息是→" + exception._3))
      .print()

    //③启动
    env.execute
  }
}
