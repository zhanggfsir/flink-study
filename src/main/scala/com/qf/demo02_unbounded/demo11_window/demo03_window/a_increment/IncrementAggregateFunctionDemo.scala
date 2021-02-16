package com.qf.demo02_unbounded.demo11_window.demo03_window.a_increment

import com.qf.Raytek
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Description：窗口函数之增量聚合函数演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月06日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object IncrementAggregateFunctionDemo {
  def main(args: Array[String]): Unit = {
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

    //③每隔4秒钟，求出不同红外测温仪所监测到的体温最高的旅客信息
    //a)获得窗口流
    val ws: WindowedStream[Raytek, Tuple, TimeWindow] = srcDataStream.keyBy("id")
      .timeWindow(Time.seconds(4)) //简介的api

    //b)聚合，求处最高的体温的旅客信息，并显示输出
    ws.reduce(new ReduceFunction[Raytek] {
      override def reduce(value1: Raytek, value2: Raytek): Raytek =
        if (value1.temperature > value2.temperature)
          value1
        else
          value2
    }).print("窗口函数之增量聚合函数→")

    //④启动
    env.execute
  }
}
