package com.cub.demo02_unbounded.demo04_transformation.sample07_self.a_common

import com.cub.Raytek
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：用户自定义函数之子类方式(参数值固化的)，需求→筛选出当天所有途径红外测温仪编号为raytek_2的旅客信息。（）<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日
  *
  * @author
  * @version : 1.0
  */
object SelfDefineFunctionDemo {
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
      }).filter(new MyFilter)
      .print("使用自定义函数子类，实现的效果→")

    //③启动
    env.execute
  }


  /**
    * 自定义过滤函数的子类
    *
    */
  class MyFilter extends FilterFunction[Raytek] {
    /**
      * 在下述方法中书写过滤业务逻辑
      *
      * 选出当天所有途径红外测温仪编号为raytek_2的旅客信息
      *
      * @param value
      * @return
      */
    override def filter(value: Raytek): Boolean = {
      "raytek_2".equals(value.id)
    }
  }

}
