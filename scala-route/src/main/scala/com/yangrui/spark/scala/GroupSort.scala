package com.yangrui.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object GroupSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupSort").setMaster("local")
    val sc = new SparkContext(conf)
    val linesRDD = sc.textFile("C:\\Users\\admin\\Desktop\\222.txt");
    val keyValueRDD = linesRDD.map(lines => {
      val spiltsLInes = lines.split(" ")
      (spiltsLInes(0), spiltsLInes(1))
    })
    val groupBykeyRDD = keyValueRDD.groupByKey()
    val afterTakeRDD = groupBykeyRDD.map(lines => {
      val tuple1 = lines._1
      val tuple2 = lines._2
      val list = tuple2.toList.sorted.reverse.take(3)
      (tuple1, list)
    })
    afterTakeRDD.foreach(lines=> println(lines._1+"     :"+lines._2))
    //    groupBykeyRDD.foreach(lines=>{
    //      println(lines._1+"  ",lines._2)
    //    }
    //    )
    sc.stop()
  }
}
