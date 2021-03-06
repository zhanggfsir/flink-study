package com.cub.demo01_bounded.sample02_srctarget_diff

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Description：计算指定源目录下所有文件中单词出现的次数。(源→hdfs, 目的地→windows(local))<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月24日  
  *
  * @author
  * @version : 1.0
  */
object BoundedFlowDemo {

  def main(args: Array[String]): Unit = {
    //步骤：

    //①执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    //②计算，显示，或是保存结果
    //a）导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._

    //b)迭代计算
    env.readTextFile("hdfs://ns1/flink/input")
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print
  }

}
