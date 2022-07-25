package com.victor.sc

import java.util.Date


object FunctionDemo {

  def main(args: Array[String]): Unit = {

    //递归使用
    for (i <- 1 to 10) {
      println(i + "的阶乘是:" + recursion(i))
    }

    //高阶函数应用
    println(apply(layout, 5))

    //匿名函数
    var inc = (i: Int) => i + 1
    println(inc(6))

    val date = new Date()
    val logWithDateBound = log(date, _: String)
    logWithDateBound("HELLO")
    logWithDateBound("WORLD")
    logWithDateBound("RELX")


    //模式匹配+匿名函数
    var matchFun = (x: Int) => x
    match {
      case 1 => 1
      case 2 => 2
      case 3 => 3
      case _ => "Any"
    }

    println(matchFun(1))

  }


  /**
   * 递归函数
   *
   * @param n
   * @return
   */
  def recursion(n: BigInt): BigInt = {
    if (n <= 1) {
      1
    } else {
      n * recursion(n - 1)
    }
  }

  /**
   * 高阶函数
   *
   * @param f
   * @param v
   * @return
   */
  def apply(f: Int => String, v: Int) = f(v)

  def layout[T](x: T) = "Hello:" + x

  /**
   * 偏应用函数
   *
   * @param date
   * @param message
   */
  def log(date: Date, message: String) = {
    println(date + "----" + message)
  }


}
