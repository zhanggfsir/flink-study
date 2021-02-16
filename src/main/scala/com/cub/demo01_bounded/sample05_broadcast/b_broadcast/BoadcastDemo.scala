package com.cub.demo01_bounded.sample05_broadcast.b_broadcast

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
  * Description：DataSet进行join操作使用broadcast来进行优化<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月03日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object BoadcastDemo {
  def main(args: Array[String]): Unit = {
    //需求：有两个有界流，其中一个有界流中存储的是性别信息，另一个有界流存储的是学生的信息，需要将学生的信息完整显示。
    //如：有界流→ (1,'男'),(2,'女')
    //      有界流 →(101,"jackson",1,"上海"),(104,"jone",2,"天津"),(108,"leon",1,"重庆")

    println(s"线程名：${Thread.currentThread().getName}")

    //步骤：
    //①环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //②获得两个DataSet
    val ds1: DataSet[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))

    val ds2: DataSet[(Int, String, Int, String)] = env.fromElements((101, "jackson", 1, "上海"), (104, "jone", 2, "天津"), (108, "leon", 1, "重庆"))

    //③将ds1作为广播变量送往每个TaskManager进程的内存中进行处理,并输出结果
    ds2.map(new RichMapFunction[(Int, String, Int, String), (Int, String, Char, String)] {//RichMapFunction实例中的方法运行在TaskManager中的slot管理的线程上

      /**
        * 准备容器，用来存放从广播变量中获取的值
        */
      var container: mutable.Map[Int, Char] = _

      /**
        * 用来进行初始化操作的，针对一个DataSet，该方法只会执行一次
        *
        * @param parameters
        */
      override def open(parameters: Configuration): Unit = {
        //初始化全局变量container
        container = mutable.Map()

        //步骤：
        //a)获得广播变量中封装的性别信息
        val lst: java.util.List[(Int, Char)] = getRuntimeContext().getBroadcastVariable("genderInformations")

        //b)将信息拿出来，存入到Map集合中
        import scala.collection.JavaConversions._
        for (perEle <- lst) {
          container.put(perEle._1, perEle._2)
        }

      }


      /**
        * 每次分析DataSet中的一个元素时，下述方法就会触发执行一次
        *
        * @param value
        * @return
        */
      override def map(value: (Int, String, Int, String)): (Int, String, Char, String) = {
        println(s"TaskManager中slot所管理的线程，线程名：${Thread.currentThread().getName}")

        val genderFlg = value._3

        val gender = container.getOrElse(genderFlg, '×')

        (value._1, value._2, gender, value._4)
      }

    }).withBroadcastSet(ds1, "genderInformations")//将ds1当成广播变量，分发到各个TaskManager的内存中
      .print()
    //      .collect()
    //      .foreach(println)

  }
}
