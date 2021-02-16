package com.qf.demo02_unbounded.demo02_calboundeddata

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：无界流演示（使用无界流的api去分析处理离线【有界流】的数据）<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object UnboundedFlowDemo {
  def main(args: Array[String]): Unit = {

//    有界流是无界流中的一个特例。可以用无界流的API编写有界的程序。

//    即使是离线的数据，经由Flink的无界流的api，底层在计算离线数据的时候，也会将其当成流式的数据，依次装载到内存中，进行计算
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②算，输出
    import org.apache.flink.api.scala._

    env.readTextFile("a_input/hello.txt")
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print("stream→")//.setParallelism(1)

    //③启动
    env.execute(this.getClass.getSimpleName)
  }
}
