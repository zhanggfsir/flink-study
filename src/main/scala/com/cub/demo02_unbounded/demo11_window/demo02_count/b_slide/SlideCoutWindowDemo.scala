package com.cub.demo02_unbounded.demo11_window.demo02_count.b_slide

import com.cub.Raytek
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
  * Description：滑动计数窗口案例演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月06日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SlideCoutWindowDemo extends App{
  //步骤：
  //①环境
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //②源（source→Socket）

  import org.apache.flink.api.scala._

  val srcDataStream: DataStream[Raytek] = env.socketTextStream("NODE01", 6666)
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

  //③安放在北京西站各个监测口的红外测温仪，每监测到2个旅客后，统计过去4个旅客中体温最高的旅客信息。
  //a)获得窗口流
  val ws: WindowedStream[Raytek, Tuple, GlobalWindow] = srcDataStream.keyBy("id")
    .countWindow(5,2)

  //b)聚合，求处最高的体温的旅客信息，并显示输出
  ws.reduce((nowTraveller: Raytek, nextTraveller: Raytek) =>
    if (nowTraveller.temperature > nextTraveller.temperature) nowTraveller
    else nextTraveller
  ).print("滑动计数窗口的效果 → ")

  //④启动
  env.execute
}
