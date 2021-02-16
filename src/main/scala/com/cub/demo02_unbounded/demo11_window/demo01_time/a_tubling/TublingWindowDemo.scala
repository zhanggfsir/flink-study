package com.cub.demo02_unbounded.demo11_window.demo01_time.a_tubling

import com.cub.Raytek
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Description：滚动窗口演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月06日  
  *
  * @author
  * @version : 1.0
  */
object TublingWindowDemo {
  def main(args: Array[String]): Unit = {
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

    //③每隔4秒钟，求出不同红外测温仪所监测到的体温最高的旅客信息
    //a)获得窗口流
    val ws: WindowedStream[Raytek, Tuple, TimeWindow] = srcDataStream.keyBy("id")
      //.timeWindow(Time.seconds(4)) //简介的api
      //注意：若一个滑动窗口的滑动的间隔和窗口长度相同，此时，该滑动窗口就是一个滚动窗口
      .window(SlidingProcessingTimeWindows.of(Time.seconds(4), Time.seconds(4)))

    //b)聚合，求处最高的体温的旅客信息，并显示输出
    ws.reduce((nowTraveller: Raytek, nextTraveller: Raytek) =>
      if (nowTraveller.temperature > nextTraveller.temperature) nowTraveller
      else nextTraveller
    ).print("滚动时间窗口效果 → ")

    //④启动
    env.execute
  }
}
