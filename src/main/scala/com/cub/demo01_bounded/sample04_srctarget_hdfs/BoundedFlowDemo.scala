package com.cub.demo01_bounded.sample04_srctarget_hdfs

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
  * Description：计算指定源目录下所有文件中单词出现的次数。(源→hdfs, 目的地→hdfs，且：源和目的地需要动态传入)<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月24日  
  *
  * @author
  * @version : 1.0
  */
object BoundedFlowDemo {

  //  input hdfs://ns1/flink/input --output hdfs://ns1/flink/output/res.txt
  def main(args: Array[String]): Unit = {
    //前提：
    // ①拦截非法的参数
    if (args == null || args.length != 4) {
      println(
        """
          |警告！请录入参数！ --input <源的path> --output <目的地的path>
        """.stripMargin)
      sys.exit(-1)
    }


    //②获得待计算的源的path，以及计算后的目的地的path
    val tool = ParameterTool.fromArgs(args)
    val inputPath = tool.get("input")  //  getInt()
    val outputPath = tool.get("output")


    //步骤：

    //①执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //②计算，显示，或是保存结果
    //a）导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._

    //b)迭代计算
    env.readTextFile(inputPath)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty).setParallelism(3)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE) //设置最后一个算子writeAsText的并行度

    //c)正式去执行
    env.execute(this.getClass.getSimpleName)

  }

}
