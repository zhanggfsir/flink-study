package com.qf.demo02_unbounded.demo03_source.sample04_self

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：使用自定义的source实时读取指定的日志文件中的数据,送往flink 进行实时的处理，并显示结果。(代码优化)<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ReadDataFromSelfSourceDemo2 {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //②读取source,处理，显示
    import org.apache.flink.api.scala._

    env.addSource(new MySource).print("自定义source优化之后→ ")

    //③启动
    env.execute
  }


}
