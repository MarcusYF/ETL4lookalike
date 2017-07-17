package Extractor

import Util.MapUtil.map2LibsvmFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yaofan29597 on 17/4/9.
 */
object saveAsLibSVM {
  def main(args: Array[String]): Unit = {

    val Array(seedPath, savePath, label, proportion) = args
    val sc = new SparkContext(new SparkConf().setAppName("ETL4lookalikev2"))
    val num_partition = 1d
    sc.textFile(seedPath)
      .sample(false, proportion.toDouble)
      .map(map2LibsvmFormat(_, label.toInt))
      .repartition(num_partition.toInt).saveAsTextFile(savePath)
   }
}
