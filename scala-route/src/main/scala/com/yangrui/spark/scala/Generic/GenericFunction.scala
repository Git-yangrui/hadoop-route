package com.yangrui.spark.scala.Generic

object GenericFunction extends App {
  def getCard[T](context: T): String = {
    if (context.isInstanceOf[Int]) {
      "card:001" + context
    } else if (context.isInstanceOf[String]) {
      "card: this is your card ," + context

    } else {
      "card :" + context
    }
  }

  val string = getCard[String]("11111111111111")
  println(string)
  //自动推断类型
  println(getCard(100))
}
