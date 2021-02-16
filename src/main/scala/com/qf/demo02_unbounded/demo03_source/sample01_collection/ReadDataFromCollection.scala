package com.qf.demo02_unbounded.demo03_source.sample01_collection

import com.qf.Raytek
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.io.Source

/**
  * Description：使用flink的无界流的API, 从集合中读取数据，进行处理<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月26日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ReadDataFromCollection {

  def main(args: Array[String]): Unit = {
    //步骤：

    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②读取文件中的数据，转换成集合
    val lst = Source.fromFile("a_input/raytek/raytek.log")
      .getLines()
      .toList

    //③将集合中的数据封装到DataStream中去
    val dataStream: DataStream[String] = env.fromCollection(lst)


    //④对无界流数据进行迭代处理，并显示结果
    dataStream.map(perEle => {
      //raytek_3,36.389,alice,1582641129,北京西站南广场-公交站
      val arr = perEle.split(",")
      val id = arr(0).trim
      val temperature = arr(1).trim.toDouble
      val name = arr(2).trim
      val timestamp = arr(3).trim.toLong
      val location = arr(4).trim
      Raytek(id, temperature, name, timestamp, location)
    }).print("红外体温测量仪→")

    //⑤启动
    env.execute(this.getClass.getSimpleName)
  }

}
