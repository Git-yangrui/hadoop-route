package com.yangrui.spark.scala.casemathc

/**
  * Scala 的模式匹配 可以匹配多情况 比如变量的类型，集合的元素，有值或无值
  */
object CaseDEmo {
  def main(args: Array[String]): Unit = {
//    def judageGrade(grade:String){
//      grade match {
//        case "A" => println("Excellent")
//        case "B" => println("God")
//        case _ if grade=="nimei"=>println("nimeimei")
//        case _ => println(" you need work hard")
//      }
//    }
//    judageGrade("A")
//    judageGrade("C")
//    judageGrade("nimei")

    def judgeGrade(name:String,grade:String){
      grade match {
        case "A" => println("Excellent")
        case "B" => println("God")
        case badGrade if name == "leo" => println("this is :" + name + " and your grade is " + badGrade)
        case _grade if name == "JAck" => println("this is :" + name + " and your grade is " + _grade)
      }
    }


    judgeGrade("JAck","D")
    judgeGrade("leo","D")
  }
}
