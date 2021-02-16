package com.cub.demo02_unbounded.demo04_transformation.sample01_rollagg

import com.cub.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：滚动聚合算子案例演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author
  * @version : 1.0
  */
object RollingAggregationDemo {
  def main(args: Array[String]): Unit = {
    //需求：计算出迄今为止分别安放在北京西站不同场所红外测温仪测量到的最高温度值。（分组 → max）

    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(2);

    //②读取数据，计算，显示
    import org.apache.flink.api.scala._

    env.readTextFile("a_input/raytek/raytek.log")
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).keyBy("id") //分组
      .max("temperature") //求每组中的最高温度
      .print("迄今为止每组中的最高温度信息是→")


    //③启动
    env.execute

  }
}
