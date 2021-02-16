package com.cub.demo01_bounded.sample05_broadcast.a_join

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * Description：DataSet进行join演示<br/>
  * Copyright (c) ，2020 ，  <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月03日  
  *
  * @author
  * @version : 1.0
  */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    //需求：有两个有界流，其中一个有界流中存储的是性别信息，另一个有界流存储的是学生的信息，需要将学生的信息完整显示。
    //如：有界流→ (1,'男'),(2,'女')
    //      有界流 →(101,"jackson",1,"上海"),(104,"jone",2,"天津"),(108,"leon",1,"重庆")


    //步骤：
    //①环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //②获得两个DataSet
    val ds1: DataSet[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))

    val ds2: DataSet[(Int, String, Int, String)] = env.fromElements((101, "jackson", 1, "上海"), (104, "jone", 2, "天津"), (108, "leon", 1, "重庆"))

    //③进行join,计算，并输出结果
    ds1.join(ds2)
      .where(0)//添加条件，左侧DataSet中的每个tuple类型的元素中的具体值，此处0代表的是性别的flg
      .equalTo(2)//添加相等的条件，右侧DataSet中的每个tuple类型的元素中的具体值，此处2代表的是性别的flg
      .map(perEle => {
        val genderTuple = perEle._1
        val stuInfoTuple = perEle._2
        (stuInfoTuple._1, stuInfoTuple._2, genderTuple._2, stuInfoTuple._4)
      })
      .print()

    /*
去掉map时join结果集
((1,男),(101,jackson,1,上海))
((1,男),(108,leon,1,重庆))
((2,女),(104,jone,2,天津))
最终结果集
(101,jackson,男,上海)
(108,leon,男,重庆)
(104,jone,女,天津)

     */
  }
}
