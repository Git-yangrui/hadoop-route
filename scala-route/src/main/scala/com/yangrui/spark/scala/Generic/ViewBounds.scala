package com.yangrui.spark.scala.Generic

object ViewBounds extends App {

  class Person(val name: String) {
    def sayHello = println("hello m I'm" + name)

    def makeFriends(p: Person): Unit = {
      sayHello
      p.sayHello
    }
  }

  class Student(name: String) extends Person(name) {
    println("stundet init complete" + name)
  }

  class Dog(val name: String) {

  }

  class Party[T <% Person](p1:T,p2:T){
    def play={
      p1.makeFriends(p2)
    }
  }

  val person = new Person("person112222222222221")



  implicit def dog2Person (dog:Object):Person={
    if(dog.isInstanceOf[Dog]){
       var dog1=dog.asInstanceOf[Dog]
      new Person(dog1.name)
    }else {
      new Person("yangrui")
    }

  }

  val dog1=new Dog("dog1")
  dog1.sayHello


  val party=new Party[Person](person,dog1)

  party.play
}
