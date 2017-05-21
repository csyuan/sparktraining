package rdd

import breeze.linalg.{norm, DenseVector => BDV}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
/**
  * Created by slyuan on 17-4-17.
  */
object SparkTest {

  def main(args: Array[String]) : Unit = {
//    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
//    val sc = new SparkContext(conf)
//    val hdfsFile = sc.textFile("hdfs://localhost:9000/tmp/spark.txt")
//    hdfsFile.flatMap(line => line.split("\\s+")).map(word => (word,1)).
//      reduceByKey(_+_).saveAsTextFile("hdfs://localhost:9000/tmp/out")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

//    val rdd = sc.parallelize(List((1,-1),(1,2),(1,4),(1,9)),2)
//    rdd.foreach(println)

//    def seq(a:Int, b:Int): Int = {
//      println("seq: " + a + "\t" + b)
//      println(math.max(a,b))
//      math.max(a,b)
//    }
//    def comb(a:Int, b:Int): Int = {
//      println("comb: " + a + "\t" + b)
////      println(a + b)
//      a + b
//    }
//
//    rdd.aggregateByKey(0)(seq, comb).foreach(println)



//    val z = sc.parallelize(List(1,2,3,4,5,6), 2)
//    z.foreach(println)
//    println(z.aggregate(1)(seq, _+_))


//
//    val data = Array((1,1.0),(1,2.0),(1,3.0),(2,4.0),(2,5.0),(2,6.0))
//    val rdd = sc.parallelize(data,2)
//    val rdd_combine =rdd.combineByKey((v:Double) =>(v:Double,1),
//    (c:(Double,Int),v:Double) =>(c._1 + v, c._2 + 1),
//    (c1:(Double,Int), c2:(Double,Int)) => (c1._1 + c2._1, c1._2 + c2._2))
//    rdd_combine.foreach(println)


    val data = Array((1,1.0),(1,2.0),(1,3.0),(2,4.0),(2,5.0),(2,6.0))
    val rdd = sc.parallelize(data)

    rdd.groupByKey().map((p:(Int, Iterable[Double])) => (p._1, p._2.sum)).foreach(println)
    spark.close()
  }
}
