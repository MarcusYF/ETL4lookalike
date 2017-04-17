package Loader

import org.apache.spark.SparkContext


/**
 * Created by yaofan29597 on 17/3/28.
 */
object readAppFeat {
  // decode from compressedText to Struct ( tdid, Array(Set_appList, Set_appKeys) )

  def text2Struct(path: String, sc: SparkContext) = {
    val data = sc.textFile(path)
    data.map {
      line =>
        val x = line.split("\t")
        val tdid = x(0)
        val appList = x(1).split(" ").map( _.toLong )

        tdid -> appList.toSet
    }
  }

  def line2Struct(line: String) = {
    val x = line.split("\t")
    val tdid = x(0)
    val appList = x(1).split(" ").map( _.toLong )

    tdid -> appList.toSet
  }

  def struct2Line(x: (String, Set[Long])) = {
    x._1 + "\t" + x._2.mkString(" ")
  }
}
