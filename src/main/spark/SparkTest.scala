import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by slyuan on 17-4-17.
  */
object SparkTest {

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val hdfsFile = sc.textFile("hdfs://localhost:9000//tmp/spark.txt")
    hdfsFile.flatMap(line => line.split("\\s+")).map(word => (word,1)).
      reduceByKey(_+_).saveAsTextFile("hdfs://localhost:9000/tmp/out")
  }
}
