package com.cub.demo02_unbounded.demo05_sink.sample04_self

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.cub.Raytek
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions._

/**
  * Description：自定义sink演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月02日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SelfDefineSinkDemo {
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
      * 进行更新的PreparedStatement
      */
    var updateStatement: PreparedStatement = _

    /**
      * 进行插入的PreparedStatement
      */
    var insertStatement: PreparedStatement = _

    /**
      * 针对于一个DataStream，下述的方法只会执行一次，用来进行初始化的动作
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://NODE03:3306/flink?serverTimezone=UTC&characterEncoding=utf-8", "root", "88888888")

      updateStatement = conn.prepareStatement(
        """
          |update tb_raytek_result
          |set temperature=?,`timestamp`=?,location=?
          |where id=? and `name`=?
        """.stripMargin)

      insertStatement = conn.prepareStatement(
        """
          |insert into  tb_raytek_result(id,temperature,`name`,`timestamp` ,location)
          |values(?,?,?,?,?)
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
      //a）进行更新
      updateStatement.setDouble(1, value.temperature)
      updateStatement.setLong(2, value.timestamp)
      updateStatement.setString(3, value.location)
      updateStatement.setString(4, value.id)
      updateStatement.setString(5, value.name)
      updateStatement.executeUpdate()

      //b)若更新失败，进行插入
      if (updateStatement.getUpdateCount == 0) {
        insertStatement.setString(1, value.id)
        insertStatement.setDouble(2, value.temperature)
        insertStatement.setString(3, value.name)
        insertStatement.setLong(4, value.timestamp)
        insertStatement.setString(5, value.location)
        insertStatement.executeUpdate()
      }

    }

    /**
      * 针对于一个DataStream，下述的方法只会执行一次，用来进行资源的释放
      */
    override def close(): Unit = {
      if (updateStatement != null)
        updateStatement.close()

      if (insertStatement != null)
        insertStatement.close()

      if (conn != null)
        conn.close()
    }
  }

}

