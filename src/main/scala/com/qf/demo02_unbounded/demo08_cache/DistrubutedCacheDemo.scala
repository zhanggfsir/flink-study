package com.qf.demo02_unbounded.demo08_cache

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/**
  * Description：分布式缓存演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月04日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object DistrubutedCacheDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②读取hdfs上的资源，并设置到分布式缓存中(注意：需要定位到文件;程序执行完下一步，没有真正从hdfs上去读取源；最终是有TaskManager读取的)
    env.registerCachedFile("hdfs://ns1/flink/cache/gender.txt", "hdfsGenderInfo")

    //③读取socket实时发送过来的学生信息，进行计算，并输出结果
    //(101,"jackson",1,"上海"),(104,"jone",2,"天津"),(108,"leon",1,"重庆")
    env.socketTextStream("NODE01", 8888)
      .filter(_.trim.nonEmpty)
      .map(new RichMapFunction[String, (Int, String, Char, String)] {

        /**
          * 可变的Map集合，用于存放从分布式缓存中读取的学生的信息
          */
        val map: mutable.Map[Int, Char] = mutable.HashMap()


        /**
          * 分布式缓存中的数据流
          */
        var bs: BufferedSource = _

        /**
          * 执行一次，进行初始化
          *
          * @param parameters
          */
        override def open(parameters: Configuration): Unit = {
          //步骤：
          //①读取分布式缓存中存储的数据，程序执行完下一行代码，从远程hdfs上将读取到的结果下载到本地文件系统相应的目录下，然后进行读取
          var file: File = getRuntimeContext.getDistributedCache.getFile("hdfsGenderInfo")

          //②将读取到的信息封装到Map实例中存储起来
          bs = Source.fromFile(file)

          val lst = bs.getLines().toList

          for (perLine <- lst) {
            val arr = perLine.split(",")
            val genderFlg = arr(0).trim.toInt
            val genderName = arr(1).trim.toCharArray()(0)
            map.put(genderFlg, genderName)
          }

        }


        /**
          * 执行n次
          *
          * 每次处理一个元素，执行一次
          *
          * @param perStudentInfo
          * @return
          */
        override def map(perStudentInfo: String): (Int, String, Char, String) = {
          //步骤：
          //①获得学生的详细信息
          val arr = perStudentInfo.split(",")
          val id = arr(0).trim.toInt
          var name = arr(1).trim
          var genderFlg = arr(2).trim.toInt
          var address = arr(3).trim

          //②根据容器Map中存储的分布式缓存中的数据，将学生信息中的性别标识替换为真实的性别
          var gender = map.getOrElse(genderFlg, '×')

          //③将新的学生信息返回
          (id, name, gender, address)
        }


        /**
          * 执行一次，进行资源释放
          */
        override def close(): Unit = {
          //关闭文件
          if (bs != null)
            bs.close()
        }
      }).print("学生完成的信息是→")

    //④启动
    env.execute(this.getClass.getSimpleName)
  }
}
