package Transformer

/**
 * Created by yaofan29597 on 17/3/21.
 */

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{UnionRDD, RDD}


object recordReducer {

  // add record
  def appListAdd(a: Set[Long], b: Set[Long]) = {
    a ++ b
  }

  def devInfoAdd(a: Set[String], b: Set[String]) = {
    a ++ b
  }

  def aoiFeatAdd(a: Set[String], b: Set[String]) = {
    a ++ b
  }

  def merge2RDDs(a: RDD[(String, Set[Long])], b: RDD[(String, Set[Long])], sc: SparkContext, num_partition: Int) = {
    val res = new UnionRDD(sc, Array(a, b)).reduceByKey( (a, b) => a ++ b , num_partition)
    res
  }


}
