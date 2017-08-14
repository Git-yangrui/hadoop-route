package com.yangrui.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSort").setMaster("local")
    val sc = new SparkContext(conf)
    val linesRDD=sc.textFile("C:\\Users\\admin\\Desktop\\123.txt");
    val secondSortKeyRDD=linesRDD.map(lines => {
      val lineSplits=lines.split(" ")
      val secondekey=new SecondSortKeyS(lineSplits(0).toInt,lineSplits(1).toInt)
      (secondekey,lines)
    })
    val sortRDD=secondSortKeyRDD.sortByKey(false)
    val afterRDD=sortRDD.map(tuple => tuple._2)
    afterRDD.foreach(line => println(line))

    sc.stop()
  }

}
