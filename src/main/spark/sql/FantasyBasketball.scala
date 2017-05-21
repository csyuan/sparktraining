package sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by slyuan on 17-5-4.
  */
object FantasyBasketball {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FantasyBasketball")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

//    val DATA_PATH = "./data/basketball/"
//    val TMP_PATH = "./data/tmp/"
//
//    val fs = FileSystem.get(new Configuration())
//    fs.delete(new Path(TMP_PATH), true)
//
//    for(i <- 1980 to 2016) {
//      println(i)
//      val yearStats = sc.textFile(s"$DATA_PATH/leagues_NBA_$i*").repartition(sc.defaultParallelism)
//      yearStats.foreach(println)
//    }
    val data = Array(1,2,3,4,5)
    val rdd1 = sc.parallelize(data)
    val rdd2 = rdd1.mapPartitions(myfunc)
    rdd2.collect().foreach(println)


  }



  def myfunc[T](iter: Iterator[T]) : Iterator[(T,T)] = {
    var res = List[(T,T)]()
    var pre = iter.next()
    while(iter.hasNext) {
      val cur = iter.next()
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }

}
