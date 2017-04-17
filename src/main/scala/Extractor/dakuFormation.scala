package Extractor

/**
 * Created by yaofan29597 on 17/3/16.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import Loader.{readParquet, readPaths, readAppFeat, readAoiFeat, readDevFeat}
import Describer.genStatDaku
import org.apache.spark.rdd.{RDD, UnionRDD}
import com.talkingdata.dmp.util.{DateUtil, Hdfs}
import Util.{HdfsUtil, MapUtil}
import Transformer.{genDaku, recordReducer}

import org.apache.spark.mllib.util.MLUtils.loadLibSVMFile
import org.apache.spark.mllib.regression.LabeledPoint

import sys.process._
import genStageRes.genDayUnionFile_aoi

import org.apache.hadoop.fs
//import scala.collection.mutable.Set
import java.util.Date
import java.text.SimpleDateFormat




import java.io._





object dakuFormation {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {

    val Array(tillDay, numDays, savePath, num_partition) = args

//    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val sc = new SparkContext(new SparkConf().setAppName("ETL4lookalikev2"))
    val sqlContext = new SQLContext(sc)


    // 天库抓取合并
    val devPaths = readPaths.locateLogPath( Array("ta", "ad", "ga"), tillDay, numDays, sc )
    val locPaths = readPaths.locateSolarSystem(tillDay, numDays, sc)
    genStageRes.genDayUnionFile(devPaths, locPaths, savePath, num_partition.toInt, sc, sqlContext)
    genStageRes.genDayUnionFile_dev(devPaths, savePath, num_partition.toInt, sc, sqlContext)


    // 天库合并到月库
    val path0 = HdfsUtil.files(sc, savePath + "/dayUnionFile_aoi/")
    val path1 = HdfsUtil.files(sc, savePath + "/dayUnionFile_app/")
    val path2 = HdfsUtil.files(sc, savePath + "/dayUnionFile_dev/")
//    new UnionRDD(sc, path0
//      .map(_.toString)
//      .map( path => readAoiFeat.text2Struct(path, sc) ) )
//      .reduceByKey( (a, b) => recordReducer.aoiFeatAdd(a, b), num_partition.toInt)
//      .map( readAoiFeat.struct2Line(_) )
//      .saveAsTextFile(savePath + "/CombineDayUnion_aoi" + "/1-" + path0.length.toString)

//    new UnionRDD(sc, path1
//      .map(_.toString)
//      .map( path => readAppFeat.text2Struct(path, sc) ) )
//      .reduceByKey( (a, b) => recordReducer.appListAdd(a, b), num_partition.toInt)
//      .map( readAppFeat.struct2Line(_) )
//      .saveAsTextFile(savePath + "/CombineDayUnion_app" + "/1-" + path1.length.toString)

    new UnionRDD(sc, path2
      .map(_.toString)
      .map( path => readDevFeat.text2Struct(path, sc) ) )
      .reduceByKey( (a, b) => recordReducer.devInfoAdd(a, b), num_partition.toInt)
      .map( readDevFeat.struct2Line(_) )
      .saveAsTextFile(savePath + "/CombineDayUnion_dev" + "/1-" + path1.length.toString)


//    // appHash 映射到 Tag, 写出到Tag_geq1
//    val Hash2Tag = MapUtil.get_Hash2Tag("/user/fan.yao/appHash_tag", sc)
//    sc.textFile(savePath + "/CombineDayUnion_app/1-" + path1.length.toString + "/part-*")
//      .map( readAppFeat.line2Struct(_) ).map {
//      x => x._1 + "\t" + x._2.map(Hash2Tag).filter(_ != "").mkString(" ").split(" ").distinct.mkString(" ")
//    }.filter {
//      line => line.split("\t").length > 1
//    }.repartition(num_partition.toInt).saveAsTextFile(savePath + "/CombineDayUnion/Tag_geq1")

//    // Aoi 映射到 Index, 写出到Aoi
//    val Aoi2Index = MapUtil.get_Aoi2Ind("/user/fan.yao/aoi_Ind/part-*", sc)
//    val aoi = sc.textFile(savePath + "/CombineDayUnion_aoi/1-" + path0.length.toString + "/part-*").map {
//      line =>
//        val x = line.split("\t")
//        if (x.length > 1) x(0) + "\t" + x(1).split(" ").map( x => Aoi2Index(x) ).mkString(" ")
//        else x(0) + "\t" + Aoi2Index("null")
//    }.repartition(num_partition.toInt).saveAsTextFile(savePath + "/CombineDayUnion/Aoi")

    // Dev 映射到 Tag, 写出到Dev
    val Dev2Index = MapUtil.get_Dev2Tag("/user/fan.yao/model_tag.csv", sc)
    val aoi = sc.textFile(savePath + "/CombineDayUnion_dev/1-" + path0.length.toString + "/part-*").map {
      line =>
        val x = line.split("\t")
        if (x.length > 1) x(0) + "\t" + x(1).split("\\$").distinct.map( x => Dev2Index(x) ).mkString(" ")
        else x(0) + "\t" + Dev2Index("null")
    }.repartition(num_partition.toInt).saveAsTextFile(savePath + "/CombineDayUnion/Dev")



    // 合并Tag_geq1,Aoi和Dev, 写出到大库
    genDaku.writeDaku(sc, savePath, num_partition.toInt)


    // 大库的描述性统计
    val path_daku =  savePath + "/Daku/part-*"
    genStatDaku.calcStatInfo_order1(sc, path_daku, savePath + "/Daku_stat/order1")
    genStatDaku.calcStatInfo_order2(sc, path_daku, savePath + "/Daku_stat/order2")


    // 转libsvm, run LR benchmark




  }
}