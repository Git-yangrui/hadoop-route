package com.yangrui.spark.scala.casemathc

object ListMatch1 extends App{
  def greeting(arr:Array[String]): Unit ={
    arr match {
      case Array("leo") => println(" hi ,leo")
      case Array(girl1, girl2, girl3) => println("hi girls")
      case Array("leo", _*) => println("hi leo ,please tell your friends ")
      case _ => println("who are you")
    }
  }

  greeting(Array("leo","1222222222222"))
  greeting(Array("11111111111111","1222222222222","121212"))
}
