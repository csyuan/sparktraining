package mllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
//import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by slyuan on 17-5-20.
  */
object LRexample {

  def main(args: Array[String]): Unit = {
    val data_path = "./data/mllib/linearRegression_data.csv"
    val conf = new SparkConf().setAppName("lr").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile(data_path)
    val examples = data.map{line =>
      val parts = line.split(":")
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(",").map(_.toDouble)))
    }.cache()

    val numExamples = examples.count()

    val numIterations = 5000
    val stepSize = 1
    val miniBatchFraction = 1.0

    val model = LinearRegressionWithSGD.train(examples,numIterations,stepSize,miniBatchFraction)
    model.weights
    model.intercept

    val prediction = model.predict(examples.map(_.features))
    val predictionAndLabel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLabel.take(50)
    println("prediction " + " \t " + "label")
    for(i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val loss = predictionAndLabel.map{
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_+_)
    val rmse = math.sqrt(loss /numExamples)

    println(s"Test RMSE = $rmse")

    val modelPath = "./data/mllib/savedModel"
    model.save(sc, modelPath)
//    val sameModel = LinearRegressionModel.load(modelPath)

    spark.close()
  }

}
