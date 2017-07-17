package Extractor

import org.apache.spark.{SparkConf, SparkContext}
import Util.MapUtil.map2LibsvmFormat
/**
 * Created by yaofan29597 on 17/4/2.
 */
object seedMatch {

  def main(args: Array[String]) {

    val Array(seedPath, dakuPath, savePath) = args
    val sc = new SparkContext(new SparkConf().setAppName("seedMatch"))


    val seed = sc.textFile(seedPath).map (x => x -> 1)
    val save_partition = 1 //seed.count / 1000000 + 1
    val daku = sc.textFile(dakuPath)
      .map{
        l => val x = l.split("\t")
        x.head -> x.last
      }
    val matched = daku.join(seed).map{
      x => x._1 + "\t" + x._2._1
    }.repartition(save_partition.toInt).saveAsTextFile(savePath)

  }
}
