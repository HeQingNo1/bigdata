package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkSQL_UDAF_Class {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_UDAF")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //进行转换之前，需要引入隐式转换规则
    //这里的spark是SparkSession对象的名字
    import spark.implicits._
    //创建聚合函数对象
    val udaf = new MysAgeAvgClassFunction

    //将聚合函数转换为查询列
    val avgCol = udaf.toColumn.name("avgAge")

    val frame = spark.read.json("input/test1.json")
    val userSet = frame.as[UserBean]
    //应用函数
    userSet.select(avgCol).show()

    spark.stop()
  }

}

case class UserBean(name:String,age:BigInt)
case class AvgBuffer(var sum:BigInt,var count:Int) //默认是val
//声明用户自定义聚合函数(强类型)
//1)继承Aggregator,设定泛型
//2）实现方法
class MysAgeAvgClassFunction extends Aggregator[UserBean,AvgBuffer,Double] {

  /**
   * 缓冲区初始化
   * @return
   */
  override def zero: AvgBuffer = {
    //AvgBuffer("sum") = 0
    //AvgBuffer("count") = 0
    AvgBuffer(0,0)
  }

  /**
   * 聚合数据
   * @param b
   * @param a
   * @return
   */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  /**
   * 合并缓冲区
   * @param b1
   * @param b2
   * @return
   */
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  /**
   * 计算
   * @param reduction
   * @return
   */
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  /**
   * 缓冲区编码类型 product：自定义的
   * @return
   */
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  /**
   * 返回值的编码类型
   * @return
   */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
