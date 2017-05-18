package mllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.util._
import org.apache.spark.sql.SparkSession
/**
  * Created by hadoop on 17-5-18.
  */
object BiasFunc {

  private lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
      println(eps)
    }
    eps
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val kmeansData = KMeansDataGenerator.generateKMeansRDD(sc, 40,3,2,1.0,2)
    kmeansData.foreach(println)
//    kmeansData.map(_.mkString(" ")).take(10).foreach(println)

    val linearData = LinearDataGenerator.generateLinearRDD(sc, 40,3,1.0,2)
//    linearData.take(10).foreach(println)
    spark.close()
  }
}
