package com.qf.demo02_unbounded.demo09_state

import com.qf.Raytek
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


/**
  * Description：键控状态演示,flatMapWithState版本<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月04日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object KeyedStateDemo2 {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //②源（source→Socket）
    val srcDataStream: DataStream[Raytek] = env.socketTextStream("NODE01", 6666)
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
      .flatMapWithState[(Raytek, String), Double] {
      //旅客第一次经过某个红外测温仪，将他的体温信息保存起来
      case (traveller, None) => {
        val nowTemperature = traveller.temperature
        val isNormal = nowTemperature >= 36.3 && nowTemperature <= 37.2
        if (isNormal) {
          (List.empty, Some(traveller.temperature))
        } else {
          (List((traveller, s"旅客【${traveller.name}】，你好！你的体温是${nowTemperature}，不在正常范围之内36.3~37.2，请接受工作人员的处理...")), Some(traveller.temperature))
        }
      }
      case (traveller, lastTemperature) => {

        //获得状态中保存的旅客上一次的体温信息
        val lastTemp = lastTemperature.get

        val nowTemperature = traveller.temperature

        val isNormal = nowTemperature >= 36.3 && nowTemperature <= 37.2


        if (isNormal) {
          //①若体温正常的话，将旅客本次的体温和上次的体温信息进行比对
          val diffTemperature = (nowTemperature - lastTemp).abs

          if (diffTemperature > 0.8) {
            //a)若体温差>阈值 （0.8），此时直接发往目标DataStream进行后续的处理
            (List((traveller, s"旅客【${traveller.name}】，你好！你本次测得的体温是${nowTemperature}，上一次测得的体温是${lastTemp}，体温差为$diffTemperature,不在临界值0.8 之内，请接受工作人员深入的调查处理...")), Some(traveller.temperature))
          } else {
            (List.empty, Some(traveller.temperature))
          }
        } else {
          //②判断旅客的体温是否在正常范围之内，若不正常，直接发往目标DataStream进行后续的处理
          (List((traveller, s"旅客【${traveller.name}】，你好！你的体温是${nowTemperature}，不在正常范围之内36.3~37.2，请接受工作人员的处理...")), Some(traveller.temperature))
        }
      }
    }
      .print("使用flatMapWithState方式，体温异常的旅客信息是→ ")

    //④启动
    env.execute
  }
}
