package com.yangrui

import org.apache.spark._
import org.apache.spark.streaming._

object SparkSQLApp {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("NetworkWordCount").setMaster("local")

    val streamingContext=new StreamingContext(conf,Seconds(1));

    val lines=streamingContext.socketTextStream("192.168.1.100",9999);

    val words=lines.flatMap(_.split(" "))
    val tuples=words.map(word=>(word,1))
    val wordcounts=tuples.reduceByKey(_+_)
    wordcounts.print()
    streamingContext.start()             // Start the computation
    streamingContext.awaitTermination()

  }
}
