package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by slyuan on 17-4-18.
  */
object TopKMovieAnalyzer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TopKMovieAnalyzer").setMaster("local")
    val sc = new SparkContext(conf)

    val DATA_PATH = "./data/ml-1m/"
    val ratingRdd = sc.textFile(DATA_PATH + "ratings.dat").map(_.split("::")).map {x=>
      (x(0),x(1),x(2))}.cache()

    val moviesRdd = sc.textFile(DATA_PATH + "movies.dat").map(_.split("::")).map(x => (x(0),x(1)))

    val userRdd = sc.textFile(DATA_PATH + "users.dat").map(_.split("::")).map(x => (x(0),x(1)))

    val moviesMap = moviesRdd.collect().toMap

    val userMap = userRdd.collect().toMap

    //得分最高的10部电影；
    val topKMovie = ratingRdd.map{ x =>
      (x._2, (x._3.toInt, 1))
    }.reduceByKey { (x1,x2) =>
      (x1._1 + x2._1,x1._2 + x2._2)
    }.map{ x=>
      (x._2._1.toFloat / x._2._2.toFloat,x._1)
    }.sortByKey(false).take(10).map{x=>
      (x._2,x._1)
    }.map{x =>
      (moviesMap.getOrElse(x._1,null),x._2)
    }.foreach(println)

    //看过电影最多的前10个人；
    val topKmostPerson = ratingRdd.map{ x =>
      (x._1,1)
    }.reduceByKey(_+_).map{ x =>
      (x._2,x._1)
    }.sortByKey(false).take(10).map{ x =>
      (x._2,x._1)
    }.foreach(println)

    //男性看过最多的10部电影
    val maleTopKPerson = ratingRdd.map{ x =>
      (userMap.getOrElse(x._1,null),x._2)
    }.filter{ x =>
      x._1.equals("M")
    }.map { x =>
      (x._2,1)
    }.reduceByKey(_+_).map{ x=>
      (x._2,x._1)
    }.sortByKey(false).take(10).map{ x =>
      (x._2,x._1)
    }.map{ x =>
      (moviesMap.getOrElse(x._1,null),x._2)
    }.foreach(println)

    //女性看过最多的10部电影；
    val femaleTopKPerson = ratingRdd.map{ x =>
      (userMap.getOrElse(x._1,null),x._2)
    }.filter{ x =>
      x._1.equals("F")
    }.map { x =>
      (x._2,1)
    }.reduceByKey(_+_).map{ x=>
      (x._2,x._1)
    }.sortByKey(false).take(10).map{ x =>
      (x._2,x._1)
    }.map{ x =>
      (moviesMap.getOrElse(x._1,null),x._2)
    }.foreach(println)

  }


}
