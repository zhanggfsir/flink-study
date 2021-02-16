package com.cub.demo02_unbounded.demo03_source.sample05_fromelements

import com.cub.entity.{MiddleStudent, Student}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * Description：source算子之fromElements，参数是可变长的，类型可以是：基础数据类型，样例类，POJO(Plain old java object),元组。<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月28日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object FromElementsDemo {

  def main(args: Array[String]): Unit = {
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②分别设置不同的source,计算，并输出
    //a)基础数据类型
        env.fromElements(56.48, 90.34, 66.89)
          .filter(_ >= 60)
          .print("及格的学生分数→")


    //b)元组
        env.fromElements(
          ("张无忌",56.48),
          ("张三丰",90.34),
          ("张翠山",66.89))
          .filter(_._2<60)
          .print("班上不及格的学生信息→")


    //c)POJO(Plain old java object)
    //i)使用POJO类原生的写法
        env.fromElements(
          new Student("张无忌", 56.48),
          new Student("张三丰", 90.34),
          new Student("张翠山", 66.89)
        ).filter(perStu => perStu.getScore >= 65 && perStu.getScore < 95)
          .print("考分在[65,95)之间的学生信息→")

    //ii)使用lombok框架优化POJO类后
    env.fromElements(
      new MiddleStudent("张无忌", 56.48),
      new MiddleStudent("张三丰", 90.34),
      new MiddleStudent("张翠山", 66.89)
    ).filter(perStu => perStu.getScore >= 65 && perStu.getScore < 95)
      .print("使用lombok后，考分在[65,95)之间的学生信息→")

    //d)样例类
        env.fromElements(
          LittleStudent("张无忌", 56.48),
          LittleStudent("张三丰", 90.34),
          LittleStudent("张翠山", 66.89))
          .filter(_.score < 60)
          .print("样例类，班上不及格的学生信息→")


    //③启动应用
    env.execute(this.getClass.getSimpleName)

  }

}
