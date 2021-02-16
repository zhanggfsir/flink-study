package com.cub.demo02_unbounded.demo05_sink.sample04_self

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.cub.Raytek
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Description：自定义sink演示,sql语句优化版<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月03日
  *
  * @author
  * @version : 1.0
  */
object SelfDefineSinkDemo2 {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //②读取源，进行处理，最后落地到es中
    import org.apache.flink.api.scala._
    //a)计算结果
    val result: DataStream[Raytek] = env.socketTextStream("NODE01", 9999)
      .filter(_.trim.nonEmpty)
      .map(perLine => {
        val arr = perLine.split(",")
        val id: String = arr(0).trim
        val temperature: Double = arr(1).trim.toDouble
        val name: String = arr(2).trim
        val timestamp: Long = arr(3).trim.toLong
        val location: String = arr(4).trim
        Raytek(id, temperature, name, timestamp, location)
      }).filter(perTraveller => perTraveller.temperature < 36.3 || perTraveller.temperature > 37.2) //←筛选出体温异常的旅客信息

    result.print("sql优化后，体温异常的旅客信息是 →")

    //b)落地到rdbms中
    val myselfSink: RichSinkFunction[Raytek] = new MyselfRichSinkFunction()
    result.addSink(myselfSink)

    //③启动
    env.execute(this.getClass.getSimpleName)
  }


  /**
    * 自定义的Sink类
    */
  class MyselfRichSinkFunction extends RichSinkFunction[Raytek] {
    /**
      * 连接
      */
    var conn: Connection = _
    /**
      * 进行插入或是更新的PreparedStatement
      */
    var insertOrUpdateStatement: PreparedStatement = _


    /**
      * 针对于一个DataStream，下述的方法只会执行一次，用来进行初始化的动作
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://NODE03:3306/flink?serverTimezone=UTC&characterEncoding=utf-8", "root", "88888888")

      insertOrUpdateStatement = conn.prepareStatement(
        """
          |
          |insert into  tb_raytek_result(id,temperature,`name`,`timestamp` ,location)
          |values(?,?,?,?,?)
          |on duplicate key update temperature=?,`timestamp`=?,location=?
          |
        """.stripMargin)

    }

    /**
      * 每次处理DataStream中的一个元素，下述的方法就执行一次
      *
      * @param value   当前待处理的元素
      * @param context 上下文信息
      */
    override def invoke(value: Raytek, context: SinkFunction.Context[_]): Unit = {
      //步骤：

      //给用于插入的占位符赋值
      insertOrUpdateStatement.setString(1, value.id)
      insertOrUpdateStatement.setDouble(2, value.temperature)
      insertOrUpdateStatement.setString(3, value.name)
      insertOrUpdateStatement.setLong(4, value.timestamp)
      insertOrUpdateStatement.setString(5, value.location)

      //给用于更新的占位符赋值，注意：序号从1开始连续计数
      insertOrUpdateStatement.setDouble(6, value.temperature)
      insertOrUpdateStatement.setLong(7, value.timestamp)
      insertOrUpdateStatement.setString(8, value.location)


      //执行（将sql语句传给远程的db server，db server会自动进行判断，若存在就更行，否则，执行插入）
      insertOrUpdateStatement.executeUpdate()

    }

    /**
      * 针对于一个DataStream，下述的方法只会执行一次，用来进行资源的释放
      */
    override def close(): Unit = {
      if (insertOrUpdateStatement != null)
        insertOrUpdateStatement.close()

      if (conn != null)
        conn.close()
    }
  }

}

