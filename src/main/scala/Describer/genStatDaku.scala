package Describer

import org.apache.spark.SparkContext
import Util.MapUtil.map2Pair

/**
 * Created by yaofan29597 on 17/4/3.
 */
object genStatDaku {

  def record_count(sc: SparkContext, path: String) = {
    sc.textFile(path).count()
  }

//  def calcStatInfo_order0(sc: SparkContext, path: String, save_path: String): Unit = {
//    val feature_count = sc.textFile(path).flatMap {
//      l =>
//        l.split("\t").last.trim.split(" ")
//    }.countByValue.toSeq.sortWith(_._2 > _._2).map(x => x._1 + "," + x._2.toString)
//
//    val num_tot = record_count(sc, path)
//    val res = feature_count.+:(num_tot.toString)
//
//    sc.parallelize(res, 1).saveAsTextFile(save_path)
//  }

  def calcStatInfo_order1(sc: SparkContext, path: String, save_path: String): Unit = {
    val feature_count = sc.textFile(path).flatMap {
      l =>
        l.split("\t").last.trim.split(" ")
    }.countByValue.toSeq.sortWith(_._2 > _._2).map(x => x._1 + "," + x._2.toString)

    val num_tot = record_count(sc, path)
    val res = feature_count.+:(num_tot.toString)

    sc.parallelize(res, 1).saveAsTextFile(save_path)
  }

  def calcStatInfo_order2(sc: SparkContext, path: String, save_path: String): Unit = {
    val res = sc.textFile(path).flatMap {
      l =>
        val x = l.split("\t").last.trim.split(" ").map(_.toInt)
        map2Pair(x)
    }.countByValue.toSeq.sortWith(_._2 > _._2).map(x => "(" + x._1._1 + "," + x._1._2 + ")" + " " + x._2.toString)


    sc.parallelize(res, 1).saveAsTextFile(save_path)


  }



}
