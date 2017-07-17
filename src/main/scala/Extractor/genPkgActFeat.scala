package Extractor

import java.text.SimpleDateFormat
import java.util.Date

import Loader.readPaths
import Loader.readPaths.Paths
import com.talkingdata.dmp.util.{Hdfs, DateUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.rdd.{RDD, UnionRDD}
import com.talkingdata.dmp.util.Util._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.{RDD, UnionRDD}

/**
 * Created by yaofan29597 on 17/5/25.
 */
object genPkgActFeat {

  def map2Str(t: Map[Long, Int]) = {
    t.map {
      case (k, v) => k + ":" + v
    }.mkString(" ")
  }

  def countActivePkg(t: Seq[Long]) = {
    val res = Map[Long, Int]()
    for (name <- t) {
      if (res.keySet.contains(name)) {
        res(name) += 1
      } else {
        res += name -> 1
      }
    }
    map2Str(res)
  }
  def sortByDate(feat:String) = {
    val x = feat.split(",")
    x.map{
      token => val y = token.split("\\|")
        y.head.toInt -> y.last
    }.sortWith(_._1 < _._1)
      .map{
      case(day, feat) => day+"|"+feat
    }.mkString(",")
  }


  def locateAdt(tillDay: String, numDays: String, sc: SparkContext) = {
    val paths = "/data/parquet/talog/day/"
    val DateFormat = new SimpleDateFormat("yyyy-MM-dd")
    Paths(paths, DateFormat.parse(tillDay), numDays.toInt)
  }

  def countByHour(ts:String, start:Long, end:Long) = {
    ts.split(",")
      .map(_.toLong)
      .filter( x => x>=start & x<=end)
      .map( x => (x-start).toFloat/(end-start)*24 )
      .map( _.toInt ).distinct
      .sortWith(_ < _)
      .mkString(",")
  }

  def genDayActWithTime(sc:SparkContext, sqlContext: SQLContext) = {


    val day = (0 to 23).map(n => "%02.0f".format(n.toFloat)).toArray
      .map("/datascience/etl2/extract/adt/2017/05/02/"+_+"/*.parquet")
      .map(sqlContext.read.parquet(_))
      .map{
      sample =>
        sample.map {
          row =>
            val tdid = row.getAs[String]("tdid")
            val time = row.getAs[Long]("receiveTime")
            val app = row.getAs[Row]("app")
            val appName = app.getAs[String]("appName")
            (tdid+":"+appName, time.toString)
        }
    }

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val start = format.parse("2017050200000").getTime()
    val end = format.parse("20170502235959").getTime()

    new UnionRDD(sc, day).reduceByKey( (a, b) => a + "," + b ).map{
      case ob@(info, time) => info + "\t" + countByHour(time, start, end)
    }.coalesce(400).saveAsTextFile("/user/fan.yao/haha/test/day/3")
  }

  def reduceByTdid(sc:SparkContext, path:String, savePath:String) = {
    sc.textFile(path).flatMap {
      line => line.split("\t") match {
        case Array(id_app, feat) => Some(id_app.split(":").head -> feat.split(","))
        case _ => None
      }
    }.reduceByKey( (a, b) => a ++ b ).map{
      case (id, feat) => id + "\t" + feat.mkString(",")
    }.coalesce(400).saveAsTextFile(savePath)
  }

  def genPkgAct(sc:SparkContext, sqlContext: SQLContext, tillDay:String, numDays:String, num_partition:String, savePath:String) = {
    val Paths = locateAdt(tillDay, numDays, sc)
    val prefix = Paths.zipWithIndex.toMap

    val all = Paths.map(_.toString).map {

      path =>
        val df = sqlContext.read.parquet(path)
        df.map{

          row =>
            // adlog抽取可能发生奇怪的NullPointerException, 没找出bug在哪, 在此加try catch保障

            val tdid = row.getAs[String]("deviceId")
            val seqs = row.getAs[Seq[Row]]("seq")
            val appkeys = for (seq <- seqs) yield seq.getAs[String]("packageName")
            val date = prefix.getOrElse(path, 0) + 1

            try {
              val feat = countActivePkg(appkeys.map(x => hash(x)))
              tdid -> (date + "|" + feat)
            } catch {
              case ex:Exception =>
                val feat = ""
                tdid -> "Null"
            }

        }.filter(_._2 != "Null")

    }

    new UnionRDD(sc, all).reduceByKey((a, b) => a + "," + b)
      .map{ case (id, feat) => id + "\t" + sortByDate(feat)}
      .repartition(num_partition.toInt).saveAsTextFile(savePath)


  }

  def main(args: Array[String]): Unit = {

    val Array(tillDay, numDays, savePath, num_partition) = args

    val sc = new SparkContext(new SparkConf().setAppName("genPkgActivFeat"))
    val sqlContext = new SQLContext(sc)

//    genDayActWithTime(sc, sqlContext)
//    reduceByTdid(sc, "/user/fan.yao/haha/test/day/3/part-*", "/user/fan.yao/haha/test/day/ku/1")

    genPkgAct(sc, sqlContext, tillDay, numDays, num_partition, savePath)




  }
}



