package sql

//import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * Created by slyuan on 17-4-22.
  */
object MovieUserStat {
  case class User(userID: String, gender: String, age: String, occupation: String, zipcode: String)

  def main(args: Array[String]): Unit = {
    //在2.0版本之前，使用Spark必须先创建SparkConf和SparkContext，代码如下：
    //set up the spark configuration and create contexts
    val conf = new SparkConf().setAppName("MovieUserStat").setMaster("local")
      .set("spark.sql.shuffle.partitions", "3") // 修改reduce task 数目（默认是200）
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
    val userRdd = users.map(_.split("::")).map( p => User(p(0), p(1), p(2), p(3), p(4)))
    val userDF = userRdd.toDF()
//    val userDF = spark.createDataset(userRdd)

   /* //action
    userDF.limit(2).toJSON.foreach(str => println(str))
    userDF.printSchema()
    userDF.collect().take(2).foreach(println)

    //transformation
    userDF.select("userID","age").show()
    userDF.selectExpr("userID","ceil(age/10) as newAge").show(2) //ceil 取整
    //userDF.selectExpr("max(age)", "min(age)", "avg(age)").show()
    //import org.apache.spark.sql.functions._ 该包必须引入
    userDF.select(max('age), min('age), avg('age)).show()
    userDF.filter(userDF("age") > 30).show()
    userDF.filter("age > 30 and occupation = 10").show()

    //混用select & filter
    userDF.select("userID","age").filter("age > 30").show()
    userDF.filter("age > 30").select("userID","age").show()

    //groupBy
    userDF.groupBy("age").count().show()
    userDF.groupBy("age","occupation").count().show()

    //agg聚集
    userDF.groupBy("age").agg(count('gender),countDistinct('occupation)).show()
    userDF.groupBy("age").agg("gender" -> "count","occupation" ->"count").show() //，可用聚集函数：avg, max, min, sum, count*/

    /**
      * Method 2: 通过反射方式，为RDD注入schema，将其变换为DataFrame
      */
//    val schemaString = "userID gender age occupation zipcode"
//    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,nullable=true)))
//    val userRdd2 = users.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim))
//    val userDataFrame2 = spark.createDataFrame(userRdd2, schema)
//
//    userDataFrame2.write.mode(SaveMode.Overwrite).parquet("./src/main/resources/parquet")
//    userDataFrame2.write.mode(SaveMode.Overwrite).json("./src/main/resources/json")
//
//    val userDataFrame3 = spark.sqlContext.read.json("./src/main/resources/json/*")
//    val userDataFrame4 = spark.sqlContext.read.format("json").load("./src/main/resources/json/*")

    //join
    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")
    val ratingSchemaString = "userID movieID Rating Timestamp"
    val ratingSchema = StructType(ratingSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable=true)))
    val ratingRDD = ratingsRdd.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim))
    val ratingDF = spark.createDataFrame(ratingRDD, ratingSchema)

    ratingDF.printSchema()

    val mergeDF = ratingDF.filter("movieID=2116").join(userDF,"userID")
      .select("gender","age").groupBy("gender","age").count().show()
//    //更多join,支持inner,outer,left_outer,right_outer,semijoin
//    val mergeDF2 = ratingDF.filter("movieID=2116").join(userDF, userDF("userID") === ratingDF("userID"),"inner")
//      .select("gender","age").groupBy("gender","age").count().show()

    //临时表
    userDF.createOrReplaceTempView("users")
    val groupedUsers = spark.sqlContext.sql("select gender,age ,count(*) as n from users group by gender,age")
    groupedUsers.show()

    //支持常用的RDD Operation
    userDF.map(u => (u.getAs[String]("userID").toLong, u.getAs[String]("age").toInt + 1.1)).take(10).foreach(println)



    spark.close()
  }
}
