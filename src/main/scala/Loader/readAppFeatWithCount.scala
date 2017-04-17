package Loader

import org.apache.spark.SparkContext

/**
 * Created by yaofan29597 on 17/3/23.
 */

import scala.collection.mutable.Set

object readAppFeatWithCount {

  // decode from compressedText to Struct ( tdid, Array(Set_appList, Set_appKeys) )

  def text2Struct(path: String, sc: SparkContext) = {
    val data = sc.textFile(path)
    data.map {
      line =>
        val x = line.split("\t")
        val tdid = x(0)
        val y = x(1).split("\\|")
        val appList = y(0)
        val appKeys = y(1)

        val appList_set = try {
          Set[(Long, Int)]() ++ appList.split(" ").map {
            token => val x = token.split("c")
              x(0).toLong -> x(1).toInt
          }
        } catch {
          case ex: Exception => Set[(Long, Int)]()
        }

        val appKeys_set = try {
          Set[(Long, Int)]() ++ appKeys.split(" ").map {
            token => val x = token.split("c")
              x(0).toLong -> x(1).toInt
          }
        } catch {
          case ex: Exception => Set[(Long, Int)]()
        }

        tdid -> Array(appList_set, appKeys_set)
    }
  }

  def struct2Text(x: (String, Array[Set[(Long, Int)]])) = {
      val y = x._2(0).map {
        case (l, i) => l.toString + "c" + i.toString
      }.mkString(" ")
      val z = x._2(1).map {
        case (l, i) => l.toString + "c" + i.toString
      }.mkString(" ")
      x._1 + "\t" + y + "|" + z
  }
}
