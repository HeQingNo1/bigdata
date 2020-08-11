package spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc = new SparkContext(conf)

    //TODO 创建累加器
     val acc = new WordAccumulator
    //TODO 注册累加器
    sc.register(acc)

    val dataRDD: RDD[String] = sc.makeRDD(Array("hello", "hi", "spark", "scala"), 2)
   // val accumulator: Any = sc.longAccumulator
    dataRDD.foreach {
      word => {
        acc.add(word)
      }
    }

    println(acc.value)

    sc.stop()
  }
}
