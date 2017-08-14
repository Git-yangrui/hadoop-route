package com.yangrui.spark.scala

/**
  *
  * @param first
  * @param second
  */
class SecondSortKeyS(val first:Int,val second:Int) extends Ordered[SecondSortKeyS] with Serializable{
  override def compare(other: SecondSortKeyS): Int = {
      if(this.first - other.first !=0){
         this.first-other.first
      }else {
        this.second -other.second
      }
  }
}
