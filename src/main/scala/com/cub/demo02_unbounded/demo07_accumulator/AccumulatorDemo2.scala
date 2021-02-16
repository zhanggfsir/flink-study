package com.cub.demo02_unbounded.demo07_accumulator

import com.cub.Raytek
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：flink中使用聚合函数实现累加器的效果 （此时不能使用累加器来完成）<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月04日  
  *
  * @author
  * @version : 1.0
  */
object AccumulatorDemo2 {
  def main(args: Array[String]): Unit = {
    //业务需求：实时统计所有车次所有到站的旅客中体温正常和异常的人数

    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    env.socketTextStream("NODE01", 8888)
      .filter(_.trim.nonEmpty)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      })
      //将经过红外测温仪的每个旅客划分到不同组中去，便于使用分组计算的算子进行累计
      .map(perEle => {
      val temperature = perEle.temperature
      val isNormal = temperature >= 36.3 && temperature <= 37.2
      if (isNormal) {
        ("体温正常", 1)
      } else {
        ("体温异常", 1)
      }
    }).keyBy(0)
      .sum(1)
      .print("所有旅客体温实时监测到的情况是 →")

    //③启动
    env.execute

  }

}
