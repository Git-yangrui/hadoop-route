package com.yangrui.spark.scala.Generic

object Implicit1 extends App {

  class SpecialPerson(val name: String)

  class Student(val name: String)

  class Older(val name: String)


  implicit def objectToSpecialPerson(obj: Object): SpecialPerson = {
    if (obj.getClass == classOf[Student]) {
      val stu = obj.asInstanceOf[Student]
      new SpecialPerson(stu.name)
    } else if (obj.getClass == classOf[Older]) {
      val stu1 = obj.asInstanceOf[Older]
      new SpecialPerson(stu1.name)
    } else {
      new SpecialPerson("1111")
    }
  }
}
