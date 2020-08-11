package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))

    val wordCount = textFile.flatMap(line => line.split("\t"))
        .map((_,1))
        .reduceByKey(_+_)

    val sort = wordCount.map(x => (x._2,x._1))
        .sortByKey(false)
        .map(x => (x._2,x._1))

    sort.saveAsTextFile(args(1))

    sc.stop()
  }

}
