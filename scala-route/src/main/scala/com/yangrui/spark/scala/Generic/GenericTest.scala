package com.yangrui.spark.scala.Generic

object GenericTest extends App {

  class Student[T](val localID: T) {
    def getSchoolid(hukouid: T): String = {
      "S-" + hukouid + localID
    }
  }

  val stu = new Student[String]("12344")
  val string = stu.getSchoolid("1232333333333333333")
  println(string)

  val stu1=new Student[Int](222)
  val string1=stu1.getSchoolid(122222)

  println(string1)

  /**
    * 高阶函数
    * @param func
    * @param v
    * @return
    */
  def getSchooo(func:String=>String,v:String)={
    func(v)
  }

  val string2=getSchooo( string => {
    string +"    13313"
  },"232332")
  println(string2)
}
