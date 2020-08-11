package flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val host:String = params.get("host")
    val port:Int = params.getInt("port")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //收取socket数据流
    val textDataStream = env.socketTextStream(host, port)

    val wordCountDataSet = textDataStream.flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataSet.print()

    //执行任务
    env.execute("streamWordCountJob")
  }

}
