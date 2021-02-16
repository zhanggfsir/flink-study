package com.cub.demo02_unbounded.demo10_statebackend.b_rocksdb

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Description：状态后端之RocksDBStateBackend演示<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月05日  
  *
  * @author
  * @version : 1.0
  */
object RocksDBStateBackendDemo {
  def main(args: Array[String]): Unit = {
    //步骤：
    //①执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //设置状态后端 (下述提示api过时的原因，官方推荐使用配置文件的方式进行全局设定，不建议使用硬编码的方式来配置)
    //String checkpointDataUri, boolean enableIncrementalCheckpointing
    val rocks = new RocksDBStateBackend("hdfs://ns1/flink/state/rocksdb",true)
    //单独设置RocksDB存储的目录，若是不单独设置，目录是以java.io.tmpdir为key对应的值（路径）
    rocks.setDbStoragePath("file:///C:\\Users\\Administrator\\IdeaProjects\\flink-study\\a_data\\statebackend")

    env.setStateBackend(rocks)

    //启用checkpoint
    env.enableCheckpointing(10000)


    //②计算，输出
    import org.apache.flink.api.scala._

    env.socketTextStream("47.104.86.109", 5555)
      .flatMap(_.split("\\s+")) //这些Operator在执行时产生的中间结果是存储在TaskManager进程的内存中的
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1) //对当前的数据计算完毕后，达到了检查点触发执行的时点后，将迄今位置的结果保存到远程hdfs之上
      .print("状态后端之RocksDBStateBacket → ")

    //③启动
    env.execute(this.getClass.getSimpleName)


    //④设置应用的重启策略 （一般在配置文件中设定）
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))
  }
}
