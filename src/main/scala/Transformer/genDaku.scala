package Transformer

import Util.MapUtil
import org.apache.spark.SparkContext

/**
 * Created by yaofan29597 on 17/4/4.
 */
object genDaku {
  def writeDaku(sc: SparkContext, savePath: String, num_partition: Int): Unit = {
    val Aoi = sc.textFile(savePath + "/CombineDayUnion/Aoi/part-*").map( MapUtil.splitId_Feat )
    val Tag = sc.textFile(savePath + "/CombineDayUnion/Tag_geq1/part-*").map( MapUtil.splitId_Feat )
    val Dev = sc.textFile(savePath + "/CombineDayUnion/Dev/part-*").map( MapUtil.splitId_Feat )

    val t = Tag.leftOuterJoin(Aoi).map {
      x =>
        val tdid = x._1
        val aoi = x._2._2.getOrElse("")
        (tdid, aoi + " " + x._2._1)
    }.leftOuterJoin(Dev).map {
      x =>
        val tdid = x._1
        val dev = x._2._2.getOrElse("")
        (tdid, dev + " " + x._2._1)
    }.map {
      x => x._1 + "\t" + x._2.trim.split("\\s+").map(_.toInt).distinct.sorted.mkString(" ")
    }.repartition(num_partition).saveAsTextFile(savePath + "/Daku")
  }

}
