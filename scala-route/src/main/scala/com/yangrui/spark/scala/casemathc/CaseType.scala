package com.yangrui.spark.scala.casemathc

object CaseType {
  def main(args: Array[String]): Unit = {
    def processException (e:Exception): Unit ={
      e match {
        case e1:IllegalArgumentException =>println("illegalArgumentException----------")
        case _ =>println("other exception")
      }
    }
    processException(new IllegalArgumentException())
  }
}
