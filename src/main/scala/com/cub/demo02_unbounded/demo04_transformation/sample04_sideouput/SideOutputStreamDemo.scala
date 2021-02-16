package com.cub.demo02_unbounded.demo04_transformation.sample04_sideouput

import com.cub.Raytek
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
;

/**
  * Description：使用侧输出流来优化前一个split的案例。<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月27日
  *
  * @author
  * @version : 1.0
  */
object SideOutputStreamDemo {
  /*
    侧输出流
  1.outputTag
  ①用来对侧输出流中的每条数据进行标识
  ②后续可以使用该标识从流中取出侧输出流中的数据，进行实时的处理
  ③如何构建实例？
  val outputTag = OutputTag[String]("late-data")

  OutputTag 对侧输出流中的每条数据进行标识
  process算子启用侧输出流


2.process 算子
①官方对该算子的说明：
   * Applies the given [[ProcessFunction]] on the input stream, thereby
   * creating a transformed output stream.

   使用process算子结合方法中传入的ProcessFunction可以用来创建一个侧输出流。

②ProcessFunction：
   A function that processes elements of a stream.
   用来处理流中的元素的

③如何从当前的流中取出侧输出流中的数据？
DataStream实例.getSideOutput(OutputTag实例)
④调用process算子后，直接从DataStream实例中取出来的数据就是主输出流中的的数据

   */
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②实时读取流数据，计算，并显示结果
    import org.apache.flink.api.scala._

    //准备一个OutputTag的实例，即为：侧输出流中每个元素的标识值；注意：下述的泛型表示侧输出流中每个元素的数据类型  和  ctx.output(outputTag, value)对应
    val outputTag = new OutputTag[Raytek]("异常体温")


    //  返回主流 DataStream
    val totalStream = env.socketTextStream("47.104.86.109", 7777)
      .map(perInfo => {
        val arr = perInfo.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).process(new ProcessFunction[Raytek, Raytek]() {

      /**
        * 处理流中每个元素，对应的业务场景是：红外体温测量仪监测每个即将跨过出口的旅客
        *
        * @param value  输入
        * @param ctx    侧输出流
        * @param out    主输出流
        */                                      // #表示内部类  即 ProcessFunction的内部类
      override def processElement(value: Raytek, ctx: ProcessFunction[Raytek, Raytek]#Context, out: Collector[Raytek]): Unit = {
        //判断
        if (value.temperature >= 36.3 && value.temperature <= 37.2) {
          //体温正常，使用实例Collector直接输出到主输出流中
          out.collect(value)
        } else {
          //体温异常，使用实例Context输出到侧输出流中
          ctx.output(outputTag, value)
        }
      }
    })

    //取出侧输出流中的旅客信息，进行处理
    totalStream.getSideOutput(outputTag)
      .print("当前的红外测温仪测到的【体温偏高】旅客信息是 →")

    //取出主输出流中的旅客信息，进行处理
    totalStream.print("当前的红外测温仪测到的<体温正常>旅客信息是 →")

    //③启动
    env.execute
  }
}

/*
outputTag 在外面，运行的线程是主线程，
process 里面会程序会分发给TaskManager的slot里的线程去执行
不是同一个线程，跨线程运行。主线程一定要序列化，不序列化，是不能传的。可以观察到outputTag已经实现序列化接口了 (Ctrl + h )
从主线程送到子线程中处理
 */
