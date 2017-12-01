package test.java

import java.io.File

import scala.io.Source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zangtt on 17-8-16.
  */
object MovieMain {
  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 设置运行环境
    val conf = new SparkConf().setAppName("TestMatrix").setMaster("local[2]")
    val sc = new SparkContext(conf)

//    val P = Array(Array[Int](1,2),Array[Int](3,4),Array[Int](4,3),Array[Int](2,1))
//    val Q = Array(Array[Int](4,3),Array[Int](2,1),Array[Int](1,2),Array[Int](3,4))
    val P = Array(1,3,6,4)
    val Q = Array(5,4,9,7)
    val k = Q.length
    val PRDD = sc.parallelize(P,2)
    val QRDD = sc.parallelize(Q,2)
    val result = PRDD.zipPartitions(QRDD) {
      (rdd1, rdd2) => {
        val P = new ArrayBuffer[Int]()
        val Q = new ArrayBuffer[Int]()
        while (rdd1.hasNext && rdd2.hasNext) {
          P += rdd1.next()
          Q += rdd2.next()
        }
        var min: Int = 1000
        for (i <- 0 until (P.length)) {
          if (i == 0) {
            min = P(0)
          }
          if (P(i) < min) {
            min = P(i)
          }
          for (j <- 0 until (Q.length)) {
            if (Q(i) < min) {
              min = Q(i)
            }
          }
        }
//        min.toString.iterator
        val res = new ArrayBuffer[String]()
        res += min.toString
        res += "0"
        res.iterator
      }
    }.reduce((x,y) => {
      var res = "a"
      var res1 = x
      var res2 = y
      if (res1 < res2) {
        res = res1
      } else {
        res = res2
      }
      res
    })
    println(result)

    sc.stop()
  }

}
