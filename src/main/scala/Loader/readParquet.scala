package Loader

import org.apache.spark.sql.{Row, SQLContext}
import com.talkingdata.dmp.util.Util.hash


/**
 * Created by yaofan29597 on 17/3/20.
 */
object readParquet {
  def readDev(path: String, sqlContext: SQLContext) = {

    val df = sqlContext.read.parquet(path).select("deviceId", "info")

    df.rdd.map{
      row =>
        val tdid = row.getAs[String]("deviceId")
        val info = row.getAs[Row]("info")
        val model = info.getAs[String]("model")

        // val all_features = app_feat ++ Set((model, 1), (pix, 1), (car, 1))

        val all_features = Set(model)

        tdid -> all_features
    }
  }

  def readApp(path: String, sqlContext: SQLContext) = {

    val df = sqlContext.read.parquet(path).select("deviceId", "seq", "apps")

    df.rdd.map{
      row =>
        // adlog抽取可能发生奇怪的NullPointerException, 没找出bug在哪, 在此加try catch保障
        try {
          val tdid = row.getAs[String]("deviceId")

          val seqs = row.getAs[Seq[Row]]("seq")
          val appkeys = for (seq <- seqs) yield seq.getAs[String]("packageName")



          val app_map = row.getAs[Map[Long, Int]]("apps")
          val app_feat = app_map.keySet

          // Array(Set[(Long, Int)](), Set[(Long, Int)]()) 安装列表+宿主应用列表

          val all_features = app_feat ++ appkeys.map(hash(_)).toSet


          if (all_features.isEmpty) None else Some(tdid -> all_features)
        } catch {
          case ex:Exception => None
        }
    }.filter(_ != None).map(_.get)

  }

  def readAoi(path: String, sqlContext: SQLContext) = {
    val df = sqlContext.read.parquet(path).select("tdid", "aoiLabel")
    df.rdd.map {
      row =>
        val tdid = row.getAs[String]("tdid")
        val aoi = row.getAs[String]("aoiLabel")
        tdid -> Set(aoi)
    }

  }
}
