package Predictor

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yaofan29597 on 17/4/7.
 */
object predictByExternalModel {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("ApplyModel"))
    val Array(dakuPath, modelPath, savePath) = args

    val w = sc.textFile(modelPath).map(_.toDouble).collect
    val w_br = sc.broadcast(w)

    sc.textFile(dakuPath).map {
      l =>
        val x = l.split("\t")
        val score = x.last.trim.split(" ").map(_.toInt-1).map(w_br.value(_)).sum
        x.head -> score
    }.sortBy(_._2, false).map {
      x => x._1 + "\t" + x._2.toString
    }.saveAsTextFile(savePath)

  }
}
