import org.apache.spark.{SparkConf, SparkContext}
import Transformer.recordReducer
import scala.collection.mutable.Set

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
/**
 * Created by yaofan29597 on 17/3/27.
 */




object test {
  def main(): Unit = {
    def getTimestamp(x:String) :java.sql.Timestamp = {
      //       "20151021235349"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")

      var ts = new Timestamp(System.currentTimeMillis());
      try {
        if (x == "")
          return null
        else {
          val d = format.parse(x);
          val t = new Timestamp(d.getTime());
          return t
        }
      } catch {
        case e: Exception => println("cdr parse timestamp wrong")
      }
      return null
    }

    val ts = new Timestamp(1493633086588L)
    val date = new Date();
  }
}
