package com.cub.demo02_unbounded.demo04_transformation.sample03_split

import com.cub.Raytek
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}

/**
  * Description：使用DataStream中的transformation算子将流分成不同特征的数据。<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author
  * @version : 1.0
  */
object SplitStreamDemo {
  /*
  特别说明：（Please use side outputs instead of split/select", "deprecated since1.8.2"）
①transformation算子之split已经过时了，在Flink高版本的api中，建议使用“侧输出流”来代替
②“侧输出流”：其中放置的是主流之外的延迟或是有异常的数据
主流：存放的是正常抵达的数据

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
      })        //  split 返回 SplitStream
      .split((rayteck: Raytek) => if (rayteck.temperature >= 36.3 && rayteck.temperature <= 37.2) Seq("正常") else Seq("异常"))

    //针对不同特征的旅客，进行不同的处理
    //a)从流中取出所有体温正常的旅客信息，进行处理（显示）
    splitStream.select("正常")  // 返回 DataStream
      .print("体温正常的旅客信息→ ")

    //b)从流中取出所有体温异常的旅客信息，进行处理（显示）
    splitStream.select("异常")
      .print("体温异常的旅客信息→ ")

    //③启动
    env.execute
  }
}
