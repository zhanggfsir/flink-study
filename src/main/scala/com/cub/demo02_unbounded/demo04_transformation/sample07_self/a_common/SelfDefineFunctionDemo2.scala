package com.cub.demo02_unbounded.demo04_transformation.sample07_self.a_common

import com.cub.Raytek
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：用户自定义函数之子类方式（参数值动态传入），需求→筛选出当天所有途径红外测温仪编号为raytek_2的旅客信息。（）<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日
  *
  * @author
  * @version : 1.0
  */
object SelfDefineFunctionDemo2 {
  //  参数示例 --hostname 47.104.86.109 --port 7777 --id raytek_2
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
      }).filter(new MyFilter(id))
      .print("使用自定义函数子类之动态传参方式，实现的效果→")

    //③启动
    env.execute
  }

// todo  类也可以传参 牛逼了 一切皆函数
  /**
    * 自定义过滤函数的子类
    *
    */
  class MyFilter(id: String) extends FilterFunction[Raytek] {
    /**
      * 在下述方法中书写过滤业务逻辑
      *
      * 选出当天所有途径红外测温仪编号为raytek_2的旅客信息
      *
      * @param value
      * @return
      */
    override def filter(value: Raytek): Boolean = {
      id.equals(value.id)
    }
  }

}
