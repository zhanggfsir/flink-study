package com.qf.demo02_unbounded.demo03_source.sample05_fromjavacollection

import java.util.Collections

import com.qf.entity.MiddleStudent
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.api.scala._

/**
  * Description：使用source阶段的算子之fromCollection,将java中的集合封装到DataStream中<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object FromJavaCollectionDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②分别设置不同的source,计算，并输出

    //将java中的集合封装到DataStream
    val container: java.util.List[MiddleStudent] = new java.util.LinkedList[MiddleStudent]()
    Collections.addAll(
      container,
      new MiddleStudent("张无忌", 56.48),
      new MiddleStudent("张三丰", 90.34),
      new MiddleStudent("张翠山", 66.89)
    )


    //导入JavaConversions单例类中的隐式方法，自动将java中的集合与scala中的集合根据需求，自动进行转换
    import scala.collection.JavaConversions._
    env.fromCollection(container)
      .print("将java中的集合封装到DataStream→")


    //③启动应用
    env.execute(this.getClass.getSimpleName)
  }
}
