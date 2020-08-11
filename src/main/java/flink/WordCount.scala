package flink

import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {

    //创建流处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "src\\main\\resources\\wc.txt"
    val inputDataSet = env.readTextFile(inputPath)

    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印输出
    wordCountDataSet.print()
  }

}
