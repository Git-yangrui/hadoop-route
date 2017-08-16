package com.yangrui.spark.scala

class TestCLass(val name:String,val age:Int) {

    var name1:String = _
    var age1: Int= _

    try{
      name1="sssssss"
      println("init")

      new Thread(new Runnable {
        override def run(): Unit = {
          println("122222222222222222222222222222222")
        }
      }).start()
      println("11111111111111111111111111111111111111111")
    }finally {
      println("finally")
    }
    def test(): Unit ={
       println(name +"   "+ name1)
    }
}


object TestCLass{
  def main(args: Array[String]): Unit = {
    val testClass=new TestCLass("yangrui",12)

    testClass.test()
  }
}
