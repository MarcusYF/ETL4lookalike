package Util

import org.apache.spark.{SparkContext}
import scala.collection.mutable.ArrayBuffer
/**
 * Created by yaofan29597 on 17/3/30.
 */
object MapUtil {

  // 分割tdid 和 feature
  def splitId_Feat(line: String) = {
    val x = line.split("\t")
    if (x.length > 1) x(0) -> x(1) else x(0) -> ""
  }

  // appHash 映射到 top50000 app
  def get_Hash2top5wIndex(path: String, sc:SparkContext) = {

    val mapHash2Top = sc.textFile(path).map{
      l => val x = l.split("\\s+")
        x(2).toLong -> x(0)
    }.collectAsMap

    def m(hash: Long) = {
      mapHash2Top.get(hash) match {
        case Some(v) => v
        case None => ""
      }
    }
    m _
  }
  // appHash 映射到 Tag
  def get_Hash2Tag(path: String, sc:SparkContext) = {

    val mapHash2Tag = sc.textFile(path).map {
      line =>
        val x = line.split(",")
        x(0).toLong -> x(1)
    }.collectAsMap

    def m(hash: Long) = {
      mapHash2Tag.get(hash) match {
        case Some(v) => v
        case None => ""
      }
    }
    m _
  }

  // Aoi 映射到 Index
  def get_Aoi2Ind(path: String, sc:SparkContext) = {

    val mapAoi2Ind = sc.textFile(path).map {
      line =>
        val x = line.split(",")
        x(0) -> x(1)
    }.collect.toMap

    def m(Aoi: String) = {
      mapAoi2Ind.get(Aoi) match {
        case Some(v) => v
        case None => ""
      }
    }
    m _
  }

  def get_Dev2Tag(path: String, sc:SparkContext) = {

    val mapDev2Ind = sc.textFile(path).map {
      line =>
        val x = line.split("\t")
        x(0) -> x(1)
    }.collect.toMap

    def m(Aoi: String) = {
      mapDev2Ind.get(Aoi) match {
        case Some(v) => v
        case None => ""
      }
    }
    m _
  }

//  def test = {
//    val g = get_Hash2Tag("",null)
//    g(1L)
//  }

  // 映射有序序列x的所有二元对
  def map2Pair(x: Array[Int]) = {
    val n = x.length
    val pair = ArrayBuffer[(Int, Int)]()
    for (i <- 0 to n-2; j <- i+1 to n-1) {

      pair += ( ( x(i), x(j) ) )

    }
    pair.toArray
  }

  def map2LibsvmFormat(line: String, label: Int) = {
    val x = line.split("\t")
    val l = if (label == 1) "+1 " else "-1 "
    l + x.last.trim.split(" ").map( _+":1").mkString(" ")
  }

//  libsvm format 数据交叉特征

//  def add_cross_feat()


}
