package com.cub.demo01_bounded.sample03_srctarget_diff2

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
  * Description：计算指定源目录下所有文件中单词出现的次数。(源→本地(windows), 目的地→hdfs)<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月24日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object BoundedFlowDemo {

  def main(args: Array[String]): Unit = {
    //步骤：

    //①执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //设置所有算子的全局的并行度  todo 每个算子也可以设置并行度
    //env.setParallelism(1)


    //②计算，显示，或是保存结果
    //a）导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._

    //b)迭代计算
    env.readTextFile("a_input")
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsText("hdfs://ns1/flink/output/result.txt", FileSystem.WriteMode.OVERWRITE) // NO_OVERWRITE
      .setParallelism(1) //设置最后一个算子writeAsText的并行度  //默认 运行在windows本地 机器为6核心12线程，启动了12个线程。则会生成12个文件

    //c)正式去执行  触发执行
    env.execute(this.getClass .getSimpleName)

  }

}
