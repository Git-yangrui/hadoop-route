package com.yangrui.spark.scala.Generic

object LowerBounds extends App{
 class Person(val name:String)
  class Father(val name:String)
  class Child(name:String)  extends Father(name)

  def getIDCard[T >: Child](p:T): Unit ={
    if(p.getClass == classOf[Child]){
      println("please tell me your parents")
    }else if(p.getClass ==classOf[Father]){
      println("please sign your name to get-----------")
    }else{
      println("Sorry, you are not allowed to get id card")
    }
  }

  val person=new Person("123")
  val child=new Child("child")
  val father=new Father("father")

  getIDCard[Child](child)
}
