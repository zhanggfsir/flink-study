package com.cub.demo02_unbounded.demo04_transformation.sample07_self.a_common

import com.cub.Raytek
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：用户自定义函数之匿名方式（参数值动态传入），需求→筛选出当天所有途径红外测温仪编号为raytek_2的旅客信息。（）<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日
  *
  * @author
  * @version : 1.0
  */
object SelfDefineFunctionDemo3 {
  def main(args: Array[String]): Unit = {
    //前提：
    //①拦截非法的参数
    if (args == null || args.length != 6) {
      sys.error(
        """
          |请传入参数！--hostname <主机名> --port <端口号>  --id <红外测温仪的id>
        """.stripMargin)
      sys.exit(-1)
    }

    //②获得参数值
    val tool = ParameterTool.fromArgs(args)
    val hostname = tool.get("hostname")
    val port = tool.getInt("port")

    val id = tool.get("id")

    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    env.socketTextStream(hostname, port)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).filter(
      new FilterFunction[Raytek] {
        override def filter(value: Raytek): Boolean = {
          id.equals(value.id)
        }
      })
      .print("使用自定义【匿名】函数之动态传参方式，实现的效果→")

    //③启动
    env.execute
  }

  //  todo 当然也可以直接过滤
//  ds.filter(_.id.equals("raytek_1")).print()

}
