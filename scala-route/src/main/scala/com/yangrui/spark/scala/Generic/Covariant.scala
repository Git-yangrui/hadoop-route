package com.yangrui.spark.scala.Generic

import java.lang.reflect.ParameterizedType

/**
  * 协变
  */
object Covariant extends  App{
  class Maser

  class Professinal1 extends Maser


//  class Card[+T](val name:String){
//  }
//  def enter(card:Card[Maser]): Unit ={
//    println("you can enter the area")
//  }
//
//  var card =new Card[Maser]("test")
//  var card_1=new Card[Professinal1]("test")
//  enter(card)
//  enter(card_1)



  class Card[-T](val name:String){
  }
  def enter(card:Card[Professinal1]): Unit ={
//    type  ParameterType=java.lang.reflect.ParameterizedType
//   val ssss= card.getClass.getGenericSuperclass.asInstanceOf[ParameterType].getActualTypeArguments
//    println(ssss)
    println("you can enter the area")
  }

  var card =new Card[Maser]("test")
  var card_1=new Card[Professinal1]("test")
  enter(card)
  enter(card_1)


}
