package com.cub.demo02_unbounded.demo09_state

import java.util.concurrent.TimeUnit

import com.cub.Raytek
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
  * Description：键控状态演示<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月04日  
  *
  * @author
  * @version : 1.0
  */
object KeyedStateDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //②源（source→Socket）
    val srcDataStream: DataStream[Raytek] = env.socketTextStream("47.104.86.109", 6666)
      .filter(_.trim.nonEmpty)
      .map(perTraveller => {
        val arr = perTraveller.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      })

    //③将体温异常的旅客信息筛选出来，且针对于同一名旅客，若当前的体温和上一次的体温差超过了0.8℃，也认为该旅客需要进行后续的处理...
    srcDataStream.keyBy("name")
      .flatMap(new MyRichFlatMapFunction(0.8))
      .print("体温异常的旅客信息是→ ")

    //④启动
    env.execute
  }

  /**
    * 自定义的富函数
    *
    * @param threshold 体温变化的阈值（临界值）
    */
  class MyRichFlatMapFunction(threshold: Double) extends RichFlatMapFunction[Raytek, (Raytek, String)] {

    /**
      * 通过ValueState来存储当前旅客上一次的体温信息
      */
    var tempValueState: ValueState[Double] = _

    /**
      * 初始化
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {

      //步骤：
      //①ValueStateDescriptor，封装了ValueState中元素的类型信息
      val desc: ValueStateDescriptor[Double] = new ValueStateDescriptor("temperature", classOf[Double])

      //②注册一个ValueState
      tempValueState = getRuntimeContext.getState[Double](desc)

    }


    /**
      * 每次处理DataStream中实时产生的元素（就是当前过红外测温仪的旅客）
      *
      * @param value
      * @param out
      */
    override def flatMap(value: Raytek, out: Collector[(Raytek, String)]): Unit = {
      //步骤：
      //获得状态中保存的旅客上一次的体温信息
      val lastTemperature = tempValueState.value()

      val nowTemperature = value.temperature

      val isNormal = nowTemperature >= 36.3 && nowTemperature <= 37.2

      if (isNormal) {
        if (lastTemperature > 0) {
          //①若体温正常的话，将旅客本次的体温和上次的体温信息进行比对
          val diffTemperature = (nowTemperature - lastTemperature).abs

          if (diffTemperature > threshold) {
            //a)若体温差>阈值 （0.8），此时直接发往目标DataStream进行后续的处理
            out.collect((value, s"旅客【${value.name}】，你好！你本次测得的体温是${nowTemperature}，上一次测得的体温是${lastTemperature}，体温差为$diffTemperature,不在临界值$threshold 之内，请接受工作人员深入的调查处理..."))
          }

          //b)体温差在正常范围之内，不予干预
        }

      } else {
        //②判断旅客的体温是否在正常范围之内，若不正常，直接发往目标DataStream进行后续的处理
        out.collect((value, s"旅客【${value.name}】，你好！你的体温是${nowTemperature}，不在正常范围之内36.3~37.2，请接受工作人员的处理..."))
      }

      //更新状态值为该旅客最新的体温信息
      tempValueState.update(nowTemperature)
    }


    /**
      * 资源释放
      */
    override def close(): Unit = {

    }
  }

}
