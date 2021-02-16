package com.cub.demo02_unbounded.demo04_transformation.sample06_union

import com.cub.Raytek
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}

/**
  * Description：要求使用union算子，将上述的两种类型的DataStream合并起来，进行统一地处理，并显示结果<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author
  * @version : 1.0
  */
object StreamUnionDemo {
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
    val normalTravellers = splitStream.select("正常") //.map(per=>(per.id,per.temperature))

    //b)从流中取出所有体温异常的旅客信息
    val exceptionTravellers = splitStream.select("异常")

    //使用union将两种不同的流合并起来，处理后，显示结果  . 如果两个流类型不一致，会报错
    normalTravellers.union(exceptionTravellers)
      .print("合并后→")

    //③启动
    env.execute
  }
}
