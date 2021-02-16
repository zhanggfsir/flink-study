package com.cub.demo02_unbounded.demo03_source.sample04_self

import com.cub.Raytek
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.io.Source

/**
  * Description：使用自定义的source实时读取指定的日志文件中的数据,送往flink 进行实时的处理，并显示结果。<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ReadDataFromSelfSourceDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //②读取source,处理，显示
    import org.apache.flink.api.scala._

    env.addSource(new SourceFunction[Raytek] {
      /**
        * 计数器，用来记录文件中的数据
        */
      var cnt = 0

      /**
        * 手动控制流运行的flg
        */
      var isRunning = true


      /**
        * 在下述的方法中书写读取真实的数据源的逻辑，并通过循环发往flink处理
        *
        * @param ctx
        */
      override def run(ctx: SourceFunction.SourceContext[Raytek]): Unit = {
        //步骤：
        //a)读取文件
        val lst = Source.fromFile("a_input/raytek/raytek.log")
          .getLines()
          .toList

        //b)通过循环，发送数据

        while (cnt < lst.size && isRunning) {

          val perEle = lst(cnt)

          val arr = perEle.split(",")
          val id = arr(0).trim
          val temperature = arr(1).trim.toDouble
          val name = arr(2).trim
          val timestamp = arr(3).trim.toLong
          val location = arr(4).trim
          val instance = Raytek(id, temperature, name, timestamp, location)

          //发送
          ctx.collect(instance)

          //计数器递增
          cnt = cnt + 1
        }


      }

      /**
        * 控制流接收
        */
      override def cancel(): Unit = {
        isRunning = false
      }
    }).print("自定义source→ ")


    //③启动
    env.execute
  }


}
