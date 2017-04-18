import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * Created by slyuan on 17-4-18.
  */
object PopularMovieAnalyzer {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PopularMovieAnalyzer").setMaster("local")
    val sc = new SparkContext(conf)

    val DATA_PATH = "./data/ml-1m/"
    val users = sc.textFile(DATA_PATH + "users.dat").map(line => line.split("::"))
      .map(user => (user(0),(user(1),user(2)))).filter(_._2._1.equals("F"))
      .filter(_._2._2 >= "18").filter(_._2._2 <= "24")


    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")
    val userlist = users.map(_._1).collect()//collect用于将一个RDD转换成数组。
    val userSet = HashSet() ++ userlist
    val broadcastUserSet = sc.broadcast(userSet)

    val topKmovies = ratingsRdd.map(_.split("::")).map(x => (x(0),x(1)))
      .filter(x => broadcastUserSet.value.contains(x._1)).map(x => (x._2,1))
      .reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false)
      .map(x => (x._2,x._1)).take(10)

    val moviesRdd = sc.textFile(DATA_PATH + "movies.dat")

    val movieID2name = moviesRdd.map(_.split("::")).map(x => (x(0),x(1))).collect().toMap

    topKmovies.map( x => (movieID2name.getOrElse(x._1,null),x._2)).foreach(println)

    sc.stop()



  }

}
