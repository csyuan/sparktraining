import java.io.File

import scala.io.Source

/**
  * Created by slyuan on 17-4-17.
  */
object Default {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]) {
      val flatMapRes = Source.fromFile(new File("./data/textfile/text.txt"))
        .getLines().toList.flatMap(line => line.split("\\s"))
      val mapRes = Source.fromFile(new File("./data/textfile/text.txt"))
        .getLines().toList.map(line => line.split("\\s"))

      //最后的toList是必须加的，因为getLines方法返回的是枚举器。
    }
  }
}
