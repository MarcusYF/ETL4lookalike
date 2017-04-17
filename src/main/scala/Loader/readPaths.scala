package Loader

import java.text.SimpleDateFormat
import java.util.Date

import com.talkingdata.dmp.util.{Hdfs, DateUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

/**
 * Created by yaofan29597 on 17/3/23.
 */
object readPaths {

  def Paths(root: String, start: Date, days: Int) = {
    DateUtil.getNearDays(start, days, "weekday").map(new Path(root, _).toString).filter(Hdfs.exists)
  }

  def locateLogPath(Logs: Array[String], tillDay: String, numDays:String, sc: SparkContext) = {

    val paths = for (str <- Logs) yield "/data/parquet/" + str + "log/day/"
    val DateFormat = new SimpleDateFormat("yyyy-MM-dd")

  // 为什么读进来只有59个?
    val p1 = Paths(paths(0), DateFormat.parse(tillDay), numDays.toInt)
    val p2 = Paths(paths(1), DateFormat.parse(tillDay), numDays.toInt)
    val p3 = Paths(paths(2), DateFormat.parse(tillDay), numDays.toInt)
//    sc.parallelize(Array(p1.length, p2.length, p3.length)).repartition(1).saveAsTextFile("/user/fan.yao/ETL4/tmp")

    val res = for (i <- 0 to p1.length-1) yield (p1(i), p2(i), p3(i))
    res.toArray
  }

  def locateSolarSystem(tillDay: String, numDays:String, sc: SparkContext) = {
    val paths = "/data/datacenter/solar-system/Location/"
    val DateFormat = new SimpleDateFormat("yyyy-MM-dd")
    Paths(paths, DateFormat.parse(tillDay), numDays.toInt)
  }

}
