package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_Kafka {
  def main(args: Array[String]): Unit = {

    //spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_WordCount")

    //实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(6))

    //从kafka采集数据
    val kafkaDStream = KafkaUtils.createStream(
      streamingContext,
      "192.168.81.101:2181",
      "hq",
      Map("hq" -> 3)
    )

    //将采集的数据进行分解（扁平化) kafka信息是（K,V),K不传是NULL
    val wordDStream = kafkaDStream.flatMap(t => t._2.split(" "))

    val mapDStream = wordDStream.map((_, 1))

    val wordToSumDStream = mapDStream.reduceByKey(_ + _)

    //打印结果
    wordToSumDStream.print()

    //不能停止采集程序
    //streamingContext.stop()

    //启动采集器
    streamingContext.start()

    //Driver等待采集器的执行
    streamingContext.awaitTermination()

  }

}
