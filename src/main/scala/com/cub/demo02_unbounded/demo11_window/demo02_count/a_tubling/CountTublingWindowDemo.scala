package com.cub.demo02_unbounded.demo11_window.demo02_count.a_tubling

import com.cub.Raytek
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

/**
  * Description：计数滚动窗口演示<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月06日  
  *
  * @author
  * @version : 1.0
  */
object CountTublingWindowDemo extends App {
  //步骤：
  //①环境
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //②源（source→Socket）

  import org.apache.flink.api.scala._

  val srcDataStream: DataStream[Raytek] = env.socketTextStream("47.104.86.109", 6666)
    .filter(_.trim.nonEmpty)
    .map(perTraveller => {
      val arr = perTraveller.split(",")
      val id: String = arr(0).trim
      val temperature: Double = arr(1).trim.toDouble
      val name: String = arr(2).trim
      val timestamp: Long = arr(3).trim.toLong
      val location: String = arr(4).trim
      Raytek(id, temperature, name, timestamp, location)
    })

  //③安放在北京西站各个监测口的红外测温仪，每监测到三个旅客后，就求出这三个旅客中体温最高的旅客信息。
  //a)获得窗口流
  val ws: WindowedStream[Raytek, Tuple, GlobalWindow] = srcDataStream.keyBy("id")
    .countWindow(3)

  //b)聚合，求处最高的体温的旅客信息，并显示输出
  ws.reduce((nowTraveller: Raytek, nextTraveller: Raytek) =>
    if (nowTraveller.temperature > nextTraveller.temperature) nowTraveller
    else nextTraveller
  ).print("滚动计数窗口的效果 → ")

  //④启动
  env.execute
}
