package com.cub.demo02_unbounded.demo01_wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：无界流演示（使用flink无界流处理实时监控netcat客户端和服务器交互的数据，进行实时的计算，将计算后的结果显示出来。）<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月24日
  *
  * @author
  * @version : 1.0
  */
object UnboundedFlowDemo {
  def main(args: Array[String]): Unit = {
    //   47.104.86.109  nc -lk 8888
    // --hostname localhost --port 8888
    // --hostname 47.104.86.109 --port 8888
    //前提：
    //①拦截非法的参数
    if (args == null || args.length != 4) {
      println("警告！应该传入参数！ --hostname <主机名>  --port <端口号>")
      sys.exit(-1)
    }

    //②获得参数
    val tool = ParameterTool.fromArgs(args)

    val hostname = tool.get("hostname")
    val port = tool.getInt("port")

    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment




    //②计算，输出
    import org.apache.flink.api.scala._

    env.socketTextStream(hostname, port)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print

    //③启动
    env.execute(this.getClass.getSimpleName)

  }
}
