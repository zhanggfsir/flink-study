package com.cub.demo02_unbounded.demo06_broadcast

import com.esotericsoftware.kryo.serializers.DefaultSerializers.IntSerializer
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, IntegerTypeInfo}
import org.apache.flink.api.common.typeutils.base.CharSerializer
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

/**
  * Description：广播无界流演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月03日  
  *
  * @author
  * @version : 1.0
  */
object BroadcastUnboundedDemo {
  def main(args: Array[String]): Unit = {
    //需求：有两个无界流，其中一个无界流中存储的是性别信息，另一个无界流存储的是学生的信息，需要将学生的信息完整显示。
    //如：无界流→ (1,'男'),(2,'女')
    //      无界流 →(101,"jackson",1,"上海"),(104,"jone",2,"天津"),(108,"leon",1,"重庆")


    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②准备两个无界流
    val dstream1: DataStream[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))

    val dstream2: DataStream[(Int, String, Int, String)] = env.socketTextStream("NODE01", 8888)
      .filter(_.trim.nonEmpty)
      .map(perLine => {
        val arr = perLine.split(",")
        val id = arr(0).trim.toInt
        val name = arr(1).trim
        val genderFlg = arr(2).trim.toInt
        val address = arr(3).trim
        (id, name, genderFlg, address)
      })


    //③将存储性别信息的无界流封装成广播流

    //a)在指定广播流中每个元素的元数据信息
    //参数1→给广播元素元数据描述信息添加一个名字；
    //参数2→广播流中每个元素的key对应的类型信息，需要对应的类型是TypeInformation，BasicTypeInfo继承自TypeInformation
    //参数3→广播流中每个元素的value对应的类型信息，需要对应的类型是TypeInformation，BasicTypeInfo继承自TypeInformation
    val broadcastStateDescriptors: MapStateDescriptor[Integer, Character] =
    new MapStateDescriptor("genderInfo", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.CHAR_TYPE_INFO)

    val bcStream: BroadcastStream[(Int, Char)] = dstream1.broadcast(broadcastStateDescriptors)

    //④存储了学生的信息无界流与广播流进行connect操作，并使用自定义广播处理函数进行计算，显示结果
    //a)获得广播连接流的实例
    val bcConnectStream: BroadcastConnectedStream[(Int, String, Int, String), (Int, Char)] = dstream2.connect(bcStream)


    //b)对广播连接流中的元素进行操作,显示结果
    bcConnectStream.process[(Int, String, Char, String)](new MyBroadcastProcessFunction(broadcastStateDescriptors))
      .print("广播流效果→")

    //⑤启动
    env.execute()

  }


  /**
    * 自定义BroadcastProcessFunction的子类
    *
    * 泛型参数1：The input type of the non-broadcast side，非广播流中的元素类型，就是dstream2流中每个元素的类型，即为：(Int, String, Int, String)
    * 泛型参数2：The input type of the broadcast side，广播流中的元素类型，就是dstream1流中每个元素的类型，即为：(Int, Char)
    * 泛型参数3：The output type of the operator，经由process处理函数处理后，返回的DataStream中每个元素的类型，即为：(Int, String, Char, String)
    */
  class MyBroadcastProcessFunction(broadcastStateDescriptors: MapStateDescriptor[Integer, Character])
    extends BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)] {
    /**
      * This method is called for each element in the (non-broadcast)，
      *
      * 下述方法会执行多次，每次分析的是非广播流dstream2中的每个元素
      *
      * @param value 封装了当前学生的信息
      * @param ctx   上下文，用于读取广播变量中的值
      * @param out   用来向结果DataStream中发送处理后的结果
      */
    override def processElement(value: (Int, String, Int, String),
                                ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)]#ReadOnlyContext,
                                out: Collector[(Int, String, Char, String)]): Unit = {

      //获得当前学生信息中的性别flg
      val genderFlg = value._3

      //从广播流中获得数据
      val genderName = ctx.getBroadcastState(broadcastStateDescriptors).get(genderFlg)

      //将当前处理后的学生信息送往目标DataStream
      out.collect((value._1, value._2, genderName, value._4))
    }

    /**
      *
      * This method is called for each element in the
      * * {@link org.apache.flink.streaming.api.datastream.BroadcastStream broadcast stream}.
      *
      * 下述方法会执行多次，每次分析的是广播流bcStream中的一个元素
      *
      * @param value 当前的性别信息
      * @param ctx   上下文信息，用来设置广播变量中具体要封装的值
      * @param out   用来向结果DataStream中发送处理后的结果（性别信息已经整合到学生信息中了，该参数可以不使用）
      */
    override def processBroadcastElement(value: (Int, Char),
                                         ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)]#Context,
                                         out: Collector[(Int, String, Char, String)]): Unit = {
      val genderFlg: Int = value._1
      val genderName: Char = value._2
      ctx.getBroadcastState(broadcastStateDescriptors).put(genderFlg, genderName)
    }
  }

}
