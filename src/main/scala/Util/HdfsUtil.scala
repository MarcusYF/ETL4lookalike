package Util

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by zhang on 2017/1/22.
 */

object HdfsUtil {
  //  var configuration = new Configuration
  //  def fsc(conf:Configuration = configuration) = FileSystem.get( conf )
  def fs(sc: SparkContext) = FileSystem.get(sc.hadoopConfiguration)

  def files(sc: SparkContext, in: Path): Array[Path] = for (s <- fs(sc).listStatus(in) if s.getPath.getName != "_SUCCESS" && s.getPath.getName != "_temporary") yield s.getPath()

  def files(sc: SparkContext, in: String): Array[Path] = files(sc, new Path(in))

  def exists(sc: SparkContext, path: String) = fs(sc).exists(new Path(path)) && !fs(sc).exists(new Path(path, "_temporary"))

  def open(sc: SparkContext, path: String): FSDataInputStream = open(sc, new Path(path))

  def open(sc: SparkContext, path: Path): FSDataInputStream = fs(sc).open(path)

  def create(sc: SparkContext, path: Path): FSDataOutputStream = fs(sc).create(path)

  def create(sc: SparkContext, path: String): FSDataOutputStream = create(sc, new Path(path))

  def delete(sc: SparkContext, path: Path, recursive: Boolean = false) = {
    fs(sc).delete(path, recursive)
  }

  def size(sc: SparkContext, path: String) = fs(sc).getFileStatus(new Path(path)).getLen

  def lines(sc: SparkContext, path: String) = {
    println("read ............")
    val in = open(sc, path)
    val codec = new CompressionCodecFactory(HdfsUtil.fs(sc).getConf).getCodec(new Path(path))
    val cin = if (codec == null) in else codec.createInputStream(in)
    val buffer = new ArrayBuffer[String]()
    buffer ++= Source.fromInputStream(cin, "UTF-8").getLines
    cin.close
    buffer
  }

  def text(sc: SparkContext, path: String) = lines(sc, path).mkString("\n")

  def text(sc: SparkContext, path: String, f: String => Unit) = {
    println("read ............")
    val in = open(sc, path)
    val codec = new CompressionCodecFactory(HdfsUtil.fs(sc).getConf).getCodec(new Path(path))
    val cin = if (codec == null) in else codec.createInputStream(in)
    Source.fromInputStream(cin, "UTF-8").getLines.foreach(f)
    cin.close
  }

  def sequenceFileReader(sc: SparkContext, in: Path) = {
    val _fs = fs(sc)
    val conf = _fs.getConf
    new Reader(fs(sc), in, conf)
  }

  def summary(sc: SparkContext, path: Path) = {
    val _fs = fs(sc)
    _fs.getContentSummary(path)
  }

  def length(sc: SparkContext, path: Path) = summary(sc, path).getLength
}
