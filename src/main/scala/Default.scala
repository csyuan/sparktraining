
import scala.io.Source

/**
  * Created by slyuan on 17-4-17.
  */
object Default extends App{

    val flatMapRes = Source.fromFile("./data/textfile/spark.txt").getLines().toList.flatMap(line => line.split("\\s+"))
    val mapRes = Source.fromFile("./data/textfile/spark.txt").getLines().toList.flatMap(line => line.split("\\s+"))
    flatMapRes.foreach(println)

    //最后的toList是必须加的，因为getLines方法返回的是枚举器。
}
