package com.qf.demo02_unbounded.demo03_source.sample04_self

import com.qf.Raytek
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.io.Source

/**
  * 自定的Source
  */
class MySource extends SourceFunction[Raytek] {
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
    // 泛型参数指的是 自定义的source把数据封装成什么类型发送出去，发送给flink分布式集群处理
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
}