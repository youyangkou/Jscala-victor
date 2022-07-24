package com.victor.sc

import scala.collection.mutable
import scala.io.StdIn

object Demo {
  def main(args: Array[String]): Unit = {
    println("hello gerry")

    //tuple
    val t1=(1,2)
    println(t1._1)

    //map
    val m=mutable.Map(("name","gerry"))
    m+=("kouyy"->"good")
    println(m.get("name").get)


    //Array
    val a=new Array[Int](10)
    println(a.length)

    //if else
    var i:Int=if(2>1) 3 else 1
    println(i)

    //+=
    i+=2
    println(i)

    //expression
    val name:String = "Gerry"
    val age: Int = 30
    val address: String ="Beijing"
    println(s"基本信息如下：\n姓名:$name\n年龄:$age\n住址:$address")

    //Stdin
    val res=StdIn.readDouble()
    println(res)

    println("for循环开始")
    //for
    for(i <- 1 to  3 ; j=4-i){
      println(j)
    }

    println("嵌套for循环开始")
    //for + for
    for (i <- 1 to 10 ; j <- 1 to 10){
      println(i+j)
    }


  }

}
