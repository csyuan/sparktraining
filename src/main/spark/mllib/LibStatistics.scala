package mllib

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
/**
  * Created by slyuan on 17-5-17.
  */
object LibStatistics {

  def main(args: Array[String]) {
    val data_path = "./data/mllib/sample_stat.txt"

    val conf = new SparkConf().setAppName("stat").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile(data_path).map(_.split("\\s+")).map(f => f.map(f => f.toDouble))
    val data_vec = data.map(f => Vectors.dense(f))
    val stat1 = Statistics.colStats(data_vec)
    //基本统计信息
    println("-----stat-----")
    println(stat1.max)
    println(stat1.min)
    println(stat1.mean)
    println(stat1.normL1)
    println(stat1.normL2)

    println("-----corr-----")
    //相关系数
    val corr1 = Statistics.corr(data_vec, "pearson")
    val corr2 = Statistics.corr(data_vec, "spearman")
    println(corr1)
    println(corr2)

    //假设检验,chi-squared
    println("-----test-----")
    val v1 = Vectors.dense(43.0, 9.0)
    val v2 = Vectors.dense(44.0, 4.0)
    val c1 = Statistics.chiSqTest(v1, v2)
    println(c1)
  }
}
