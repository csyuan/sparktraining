package sql

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Created by slyuan on 17-4-22.
  */
object MovieUserStat {
  case class User(userID: String, gender: String, age: String, occupation: String, zipcode: String)

  def main(args: Array[String]): Unit = {
    //在2.0版本之前，使用Spark必须先创建SparkConf和SparkContext，代码如下：
    //set up the spark configuration and create contexts
    val conf = new SparkConf().setAppName("MovieUserStat").setMaster("local")
      .set("spark.some.config.option", "some-value")
    //    // your handle to SparkContext to access other context like SQLContext
    //    val sc = new SparkContext(sparkConf)
    //    val sqlContext = new SQLContext(sc)

    /**
      * 不过在Spark2.0中只要创建一个SparkSession就够了，
      * SparkConf、SparkContext和SQLContext都已经被封装在SparkSession当中。
      * 下面的代码创建了一个SparkSession对象并设置了一些参数。
      * 这里使用了生成器模式，只有此“spark”对象不存在时才会创建一个新对象。
      */
    // Create a SparkSession. No need to create SparkContext
    // You automatically get it as part of the SparkSession

    val spark = SparkSession
      .builder()
      .appName("MovieUserStat")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    //    spark.conf.set("spark.executor.memory", "2g")
    //    val df = spark.read.json("./src/main/resources/people.json")
    //    df.show()

    val DATA_PATH = "./data/ml-1m/"

    /**
      * Method 1: 通过显式为RDD注入schema，将其变换为DataFrame
      */
    import spark.implicits._

    val users = spark.sparkContext.textFile(DATA_PATH + "users.dat")
//    val userRdd = users.map(_.split("::"))
//    .map( p => User(p(0), p(1), p(2), p(3), p(4)))
//    val userDataFrame = userRdd.toDF()
////    val userDataFrame = spark.createDataset(userRdd)
//    userDataFrame.show()
//    userDataFrame.count()

    /**
      * Method 2: 通过反射方式，为RDD注入schema，将其变换为DataFrame
      */
    val schemaString = "userID gender age occupation zipcode"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,nullable=true)))
    val userRdd2 = users.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim))
    val userDataFrame2 = spark.createDataFrame(userRdd2, schema)
    userDataFrame2.show()


    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")
    val ratingSchemaString = "userID movieID Rating Timestamp"
    val ratingSchema = StructType(ratingSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable=true)))
    val ratingRDD = ratingsRdd.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim))
    val ratingDataFrame = spark.createDataFrame(ratingRDD, ratingSchema)


  }
}
