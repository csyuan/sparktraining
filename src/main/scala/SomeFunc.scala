/**
  * Created by hadoop on 17-5-17.
  */
object SomeFunc {

  def main(args: Array[String]) {
    val alphabet = List("A","B","C")
    val nums = List(1,2)
    val zipped = alphabet zip nums   //List((A,1), (B,2))
    println(zipped)
    println("--------------------")
    val zippedAll = alphabet.zipAll(nums,"*",-1)   //List((A,1), (B,2), (C,-1))

    println(zippedAll)
    println("--------------------")

    val zippedIndex = alphabet.zipWithIndex  //List((A,0), (B,1), (C,2))
    println(zippedIndex)
    println("--------------------")

    val (list1,list2) = zipped.unzip        //(List(A, B),List(1, 2))
    println((list1,list2))
    println("--------------------")

    val (l1,l2,l3) = List((1, "one", '1'),(2, "two", '2'),(3, "three", '3')).unzip3   //(List(1, 2, 3),List(one, two, three),List(1, 2, 3))
    println((l1,l2,l3))

  }
}
