package com.yangrui.spark.scala.Generic

object Upperbounds  extends App{
   class Person(val name:String){
     def sayHello=println("hello ,I'm "+name)
     def makeFriends(p:Person): Unit ={
       sayHello
       p.sayHello
     }
   }

   class Studeng(name:String) extends Person(name)

  class Party[T<:Person](p1:T,p2:T){
    def play=p1.makeFriends(p2)
  }

  val studeng=new Studeng("yangrui")
  val studeng1=new Studeng("mingzhu")
  val party=new Party[Studeng](studeng,studeng1)
  party.play
}
