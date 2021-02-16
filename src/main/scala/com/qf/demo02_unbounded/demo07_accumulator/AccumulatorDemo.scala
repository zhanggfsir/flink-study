package com.qf.demo02_unbounded.demo07_accumulator

import java.util.Properties

import com.qf.Raytek
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Description：flink中的累加器演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月04日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    //业务需求：需要统计本次到站的所有旅客中，总的旅客数，体温正常的旅客数，体温异常的旅客数。

    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment



    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    env.socketTextStream("NODE01", 8888)
      .filter(_.trim.nonEmpty)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).map(new MyRichMapFunction())
      .print("经过累加器处理后结果是 →")

    //③启动
    val result: JobExecutionResult = env.execute


    //④显示累加器的值
    val totalCnt = result.getAccumulatorResult[Int]("totalAcc")
    val normalCnt = result.getAccumulatorResult[Int]("normalAcc")
    val exceptionCnt = result.getAccumulatorResult[Int]("exceptionAcc")

    println(s"本次列车，旅客总数→$totalCnt，体温正常的旅客数→$normalCnt，体温异常的旅客数→$exceptionCnt")
  }

  /**
    * 自定义的富函数，因为只有富函数中，才能获得上下文的信息，才能注册累加器，否则，不能使用累加器
    */
  class MyRichMapFunction extends RichMapFunction[Raytek, (Raytek, String)] {

    /**
      * 用于统计所有旅客数的累加器
      */
    private var totalAcc: IntCounter = _

    /**
      * 用于统计体温正常旅客数累加器
      */
    private var normalAcc: IntCounter = _


    /**
      * 用于统计体温异常旅客数累加器
      */
    private var exceptionAcc: IntCounter = _


    /**
      * 调用一次， 用来进行初始化
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      //步骤：
      //a)初始化累加器
      totalAcc = new IntCounter()
      normalAcc = new IntCounter()
      exceptionAcc = new IntCounter()

      //b)注册累加器
      val context: RuntimeContext = getRuntimeContext

      context.addAccumulator("totalAcc", totalAcc)
      context.addAccumulator("normalAcc", normalAcc)
      context.addAccumulator("exceptionAcc", exceptionAcc)
    }

    /**
      *
      * 调用n次
      *
      * 依次来分析DataStream中的每个元素
      *
      * @param value 封装了当前旅客的信息
      * @return
      */
    override def map(value: Raytek): (Raytek, String) = {
      //步骤：
      //①总数累加器累加1
      totalAcc.add(1)

      //②根据当前旅客的体温进行处理
      val temperature = value.temperature
      val isNormal = temperature >= 36.3 && temperature <= 37.2

      if (isNormal) {
        //a）体温正常，体温正常的累加器累加1，为发送结果到结果DataStream中进行后续的处理
        normalAcc.add(1)
        (value, "恭喜！您的体温正常，可以通过...")
      } else {
        //b）体温异常，体温异常的累加器累加1，为发送结果到结果DataStream中进行后续的处理
        exceptionAcc.add(1)
        (value, s"抱歉！您的体温偏高,达到了$temperature，不在正常值36.3~37.2之内，请稍等...")
      }
    }

    /**
      * 调用一次， 资源释放
      */
    override def close(): Unit = super.close()
  }

}
