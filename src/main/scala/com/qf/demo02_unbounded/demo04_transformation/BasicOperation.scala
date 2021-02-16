package com.qf.demo02_unbounded.demo04_transformation

import com.mysql.fabric.xmlrpc.base.Data
import lombok.{AllArgsConstructor, NoArgsConstructor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.reflect.internal.util.Collections

object BasicOperation {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._


    val env = StreamExecutionEnvironment.getExecutionEnvironment


//   支持的数据类型

    // 1. 基础数据类型
    val numbers:DataStream[Long] = env.fromElements(1L, 2L, 3L, 4L)
    numbers.map(n => n + 1).print()

    // 2. Java和Scala元组（Tuples）
    val persons:DataStream[(String,Integer)]=env.fromElements(
      ("独孤求败", 17),
      ("睥睨天下", 23))

    persons.filter(p => p._2 > 18).print()

    // 3. 其它（Array,List,Map,Enum,等等）
    //Flink对Java和Scala中的一些特殊目的的类型也都是支持的，比如 Java的ArrayList，HashMap，Enum等等。

//    import scala.collection.JavaConversions._
//    val lst = new util.LinkedList[String]
//    Collections.addAll(lst, "jack", "marry")
//    env.fromCollection(lst).print()

    // 4. Scala样例类（case class）  // todo 把每一个样例类装到了DataStream
//    case class Employee(name:String,age:Int)
//    val emps:DataStream[Employee]=env.fromElements(
//      Employee("舍我其谁",16),
//      Employee("群雄逐鹿",24))
//    emps.filter(e=>e.age>18)




    // 5. Java简单对象（POJOs）
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public class People{
//      private String name;
//      private int age;
//    }
//    val persons:DataStream[People]=env.fromElements(
//      new People("王者荣耀",40),
//      new People("互相伤害啊",28));










    //a)基础数据类型
    env.fromElements(56.48, 90.34, 66.89)
    .filter(_ >= 60)
    .print("及格的学生分数→")




/*
实现UDF函数——更细粒度的控制流

//    1. 函数类（Function Class）
//    Flink暴露了所有udf函数的接口(实现方式为接口或者抽象类)。例如
//    MapFunction、FilterFunction、ProcessFunction等等。
    class RaytekFilter() extends FilterFunction[Raytek] {
      override def filter(value: Raytek): Boolean = {
        value.id.equals("raytek_1")
      }
    }

    // 2. 函数类
    dataStream.filter( new RaytekFilter() ).print()
    //↓ 可以将函数实现成匿名类 ↓
    dataStream.filter(new RichFilterFunction[Raytek] {
      override def filter(value: Raytek): Boolean =
        value.id.equals("raytek_1")
    })

    // 3. filter的字符串"raytek_1"还可以当作参数传进去 ↓
    class RaytekFilter(id: String) extends RichFilterFunction[Raytek] {
      override def filter(value: Raytek): Boolean = value.id.equals(id)
    }

    // 4. 调用带参数的函数类
    ds.filter(new RaytekFilter("raytek_1")).print()

    // 5. 匿名函数（Lambda Functions）
    ds.filter(_.id.equals("raytek_1")).print()


 */

    env.execute(this.getClass.getSimpleName)
  }
}
