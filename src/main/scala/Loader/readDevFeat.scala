package Loader

import org.apache.spark.SparkContext

/**
 * Created by yaofan29597 on 17/4/12.
 */
object readDevFeat {
  def text2Struct(path: String, sc: SparkContext) = {
    val data = sc.textFile(path)
    data.map {
      line =>
        val x = line.split("\t")
        val tdid = x.head
        val y = x.last.trim.split("$")
        val res = y.toSet
        tdid -> res
    }
  }

  def struct2Line(x: (String, Set[String])) = {
    x._1 + "\t" + x._2.mkString("$")
  }
}
