package rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by slyuan on 17-4-18.
  */
object MovieUserAnalyzer {

  def main(args: Array[String]) {

//    val conf = new SparkConf().setMaster("local").setAppName("MovieUserAnalyzer")
//    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("MovieUserStat")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    val DATA_PATH = "./data/ml-1m/"
    val MOVIE_ID = "2116"
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"

    val userRdd = spark.sparkContext.textFile(DATA_PATH + "users.dat")
    val ratingsRdd = spark.sparkContext.textFile(DATA_PATH + "ratings.dat")

    val users = userRdd.map(line => line.split("::")).map(x => (x(0),(x(1),x(2))))

    val ratings = ratingsRdd.map(line => line.split("::"))//.map(x => (x(0),x(1))).filter(_._2.equals(MOVIE_ID))

    val usersMovie = ratings.map(x => (x(0),x(1))).filter(_._2.equals(MOVIE_ID))

    val userRating = usersMovie.join(users)

    val userDistr = userRating.map(x => (x._2._2,1)).reduceByKey(_+_)

    userDistr.foreach(println)

    spark.sparkContext.stop()

  }

}
