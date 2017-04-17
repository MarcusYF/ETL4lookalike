package Extractor

import Transformer.recordReducer
import Loader.{readAoiFeat, readAppFeat, readDevFeat, readParquet}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.sql.SQLContext

/**
 * Created by yaofan29597 on 17/3/30.
 */
object genStageRes {
  def genDayUnionFile_app(devPaths: Array[(String, String, String)], savePath: String, num_partition: Int, sc:SparkContext, sqlContext: SQLContext): Unit = {
    devPaths.foreach { path =>
      val date = path._1.split("/").last
      val outpath = savePath + "/dayUnionFile_app/" + date
      new UnionRDD(sc, Array(readParquet.readApp(path._1, sqlContext), readParquet.readApp(path._2, sqlContext), readParquet.readApp(path._3, sqlContext)))
        .reduceByKey( (a, b) => recordReducer.appListAdd(a, b), num_partition)
        .map ( readAppFeat.struct2Line(_) )
        .saveAsTextFile(outpath)
    }
  }

  def genDayUnionFile_aoi(locPaths: Array[String], savePath: String, num_partition: Int, sc:SparkContext, sqlContext: SQLContext): Unit = {
    locPaths.foreach { path =>
      val date = path.split("/").last
      val outpath = savePath + "/dayUnionFile_aoi/" + date
      readParquet.readAoi(path, sqlContext)
        .reduceByKey( (a, b) => recordReducer.aoiFeatAdd(a, b), num_partition.toInt)
        .map( readAoiFeat.struct2Line(_) )
        .saveAsTextFile(outpath)
    }
  }

  def genDayUnionFile_dev(devPaths: Array[(String, String, String)], savePath: String, num_partition: Int, sc:SparkContext, sqlContext: SQLContext): Unit = {
    devPaths.foreach { path =>
      val date = path._1.split("/").last
      val outpath = savePath + "/dayUnionFile_dev/" + date
      new UnionRDD(sc, Array(readParquet.readDev(path._1, sqlContext), readParquet.readDev(path._2, sqlContext), readParquet.readDev(path._3, sqlContext)))
        .reduceByKey( (a, b) => recordReducer.devInfoAdd(a, b), num_partition )
        .map ( readDevFeat.struct2Line(_) )
        .saveAsTextFile(outpath)
    }
  }

  def genDayUnionFile(devPaths: Array[(String, String, String)], locPaths: Array[String], savePath: String, num_partition: Int, sc:SparkContext, sqlContext: SQLContext): Unit = {
    val num_days = devPaths.length.min(locPaths.length)

    for (i <- 0 to num_days-1) {
      val devPath = devPaths(i)
      val locPath = locPaths(i)
      val date = devPath._1.split("/").last

      val outpath1 = savePath + "/dayUnionFile_app/" + date
      new UnionRDD(sc, Array(readParquet.readApp(devPath._1, sqlContext), readParquet.readApp(devPath._2, sqlContext), readParquet.readApp(devPath._3, sqlContext)))
        .reduceByKey( (a, b) => recordReducer.appListAdd(a, b), num_partition)
        .map ( readAppFeat.struct2Line(_) )
        .saveAsTextFile(outpath1)
      val outpath2 = savePath + "/dayUnionFile_aoi/" + date
      readParquet.readAoi(locPath, sqlContext)
        .reduceByKey( (a, b) => recordReducer.aoiFeatAdd(a, b), num_partition)
        .map( readAoiFeat.struct2Line(_) )
        .saveAsTextFile(outpath2)
    }


  }
}
