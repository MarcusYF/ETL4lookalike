package Extractor

import org.apache.spark.{SparkConf, SparkContext}
import Describer.genStatDaku

/**
 * Created by yaofan29597 on 17/4/28.
 */
object genStats {
  def main(args: Array[String]) {

    val Array(path_daku, savePath, order) = args
    val sc = new SparkContext(new SparkConf().setAppName("genStats"))

    val flag = order.toInt
    if (flag>0) {
      genStatDaku.calcStatInfo_order1(sc, path_daku, savePath + "/order1")
      if (flag>1) genStatDaku.calcStatInfo_order2(sc, path_daku, savePath + "/order2")
    }

  }
}
