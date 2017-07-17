package Extractor

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.sys.process._

/**
 * Created by yaofan29597 on 17/4/25.
 */
object IdMapping {

  def main(args: Array[String]) {

    val Array(origin_type, originPath, savePath) = args
    val sc = new SparkContext(new SparkConf().setAppName("ID Mapping"))
    val sqlContext = new SQLContext(sc)

    val all = sc.broadcast(sc.textFile(originPath).collect.toSet)
    sqlContext.read.parquet("/data/datacenter/rank/infos/2017/14/2017-04-09/deviceinfos").select("deviceId", origin_type).flatMap {
      row =>
        var flag = false
        val macs = row.getAs[Seq[String]](origin_type).map(_.toLowerCase)
        for (m <- macs if !flag) {
          if (all.value.contains(m)) flag = true
        }
        if (flag) Some(row.getAs[String]("deviceId"))
        else None
    }.distinct(1).saveAsTextFile(savePath)

//    s"hadoop fs -mv /user/fan.yao/NB/tmp/part-00000 $savePath" !

//    s"hadoop fs -rm -r /user/fan.yao/NB/tmp" !

  }
}
