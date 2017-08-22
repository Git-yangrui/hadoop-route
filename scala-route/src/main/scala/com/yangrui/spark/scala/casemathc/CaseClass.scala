package com.yangrui.spark.scala.casemathc

import java.net.Socket

object CaseClass extends App {
  type nimei = Socket

  class Person

  case class Teacher(name: String, subject: String) extends Person

  case class Student(name: String, classroom: String) extends Person

  def judgeIdetify(p: Person): Unit = {
    p match {
      case Teacher(name111, subject1122) =>  println("this is teacher "+name111+"  "+subject1122)
      case Student(name, classroom) => println("this is student")
      case _ => println("unknown")
    }
  }

//  judgeIdetify(Student("sds", "sdsds"))

//  val teacher = Teacher("yangrui", "sdsd")
//  println(teacher.name)
//  println(teacher.name)

  judgeIdetify(Teacher("yangrui","123132131"))
}
