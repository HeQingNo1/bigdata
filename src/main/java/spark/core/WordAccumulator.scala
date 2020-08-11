package spark.core

import java.util

import org.apache.spark.util.AccumulatorV2

class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]] {

  private val list = new util.ArrayList[String]()

  //当前的累加器是否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  override def add(v: String): Unit = {
    if(v.contains("h")){
      list.add(v)
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器的值
  override def value: util.ArrayList[String] = {
    list
  }
}
