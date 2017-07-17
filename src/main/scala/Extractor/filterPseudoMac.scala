package Extractor

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yaofan29597 on 17/4/28.
 */
object filterPseudoMac {
  def main(args: Array[String]) {

    val Array(originPath, savePath) = args
    val sc = new SparkContext(new SparkConf().setAppName("filterPseudoMac"))


    val true_mac = sc.broadcast(sc.textFile("/user/fan.yao/NB/true_mac").collect.toSet)

    sc.textFile(originPath).filter {
      l => true_mac.value.contains(l.take(8))
    }.repartition(1).saveAsTextFile(savePath)

  }

}