package com.cub.demo02_unbounded.demo03_source.sample02_file

import com.cub.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * Description：使用Flink无界流的api从日志文件中实时读取红外测温仪采集到的旅客的体温信息，计算，并显示出来<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ReadDataFromLogFile {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②读取日志文件，并迭代计算，显示结果
    import org.apache.flink.api.scala._
    env.readTextFile("a_input/raytek/raytek.log")
      .map(perInfo => {
        //raytek_1,36.3,jack,1582641121,北京西站-北广场
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).print("北京西站红外测温仪实时监测的信息→")


    //③启动无界流应用
    env.execute(this.getClass.getSimpleName)
  }

}
