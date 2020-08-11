package spark.streaming

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

//自定义采集器
object SparkStreaming_MyReceiver {
  def main(args: Array[String]): Unit = {

    //spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_WordCount")

    //实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    //使用自定义采集器采集数据
    val receiverDStream = streamingContext.receiverStream(new MyReceiver("192.168.81.101",999))

    //将采集的数据进行分解（扁平化)
    val wordDStream = receiverDStream.flatMap(_.split(" "))

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

//声明采集器
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  var socket:java.net.Socket = null

  def receive() ={

    socket = new java.net.Socket(host,port)

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    var line:String = null

    while ((line = reader.readLine()) != null ){

      //将采集的数据存储到采集器的内部进行转换
      if ( "END".equals(line) ){
        //return
      }else{
        this.store(line)
      }
    }

  }
  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {
    if ( socket != null ){
      socket.close()
      socket = null
    }
  }
}