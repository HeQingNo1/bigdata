package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object test extends App {
   val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
   //val sc = new SparkContext(sparkConf)

   //val dataRDD: RDD[(Int,Int)] = sc.makeRDD(Array((1,2),(2,2),(3,3)), 2)
  //dataRDD.reduceByKey(_+_,2)

}
