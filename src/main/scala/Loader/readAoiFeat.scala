package Loader

import org.apache.spark.SparkContext

/**
 * Created by yaofan29597 on 17/3/29.
 */
object readAoiFeat {
  def text2Struct(path: String, sc: SparkContext) = {
    val data = sc.textFile(path)
    data.map {
      line =>
        val x = line.split("\t")
        val tdid = x(0)
        val y = x(1).split(" ")
        val res = y.toSet - "null"
        tdid -> res
    }
  }

  def struct2Line(x: (String, Set[String])) = {
    x._1 + "\t" + x._2.mkString(" ")
  }
}
