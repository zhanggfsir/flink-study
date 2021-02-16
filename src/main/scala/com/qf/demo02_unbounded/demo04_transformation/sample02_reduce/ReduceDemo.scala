package com.qf.demo02_unbounded.demo04_transformation.sample02_reduce

import com.qf.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：求出安放在北京西站各个位置的红外测温仪迄今所测得的最早的时间，以及测得的最高温度。<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    env.socketTextStream("47.104.86.109", 7777)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).keyBy("id") //根据红外测温仪的id进行分组
      // 求各个分组中的最早的时间，以及最高的温度
      .reduce((before: Raytek, after: Raytek) => {
      val maxTemperature = Math.max(before.temperature, after.temperature)
      val minTimestamp = Math.min(before.timestamp, after.timestamp)
      Raytek(before.id, maxTemperature, "", minTimestamp, "")
    }).print("当前红外测温仪迄今为止测得的最高的体温、最早的时间 →")

    //③启动
    env.execute
  }
}
