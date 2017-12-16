package zz.test

import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.rest.RestClient
import org.elasticsearch.spark.cfg.SparkSettingsManager


class D {

}

class HardToBuild1(sparkSession: SparkSession) {
//  println("Jestem Nowy")

//  lazy val rr:RestClient = {
//    val sm = new SparkSettingsManager().load(sparkSession.sparkContext.getConf)
//    println(sm.asProperties())
//    new RestClient(sm)
//  }
//
//  def getSomething(): Unit = {
//    println(rr.indexExists("aaa"))
//    println(rr.stats().docsAccepted)
//  }

}

object HardToBuild {
  import zz.test.HardToBuild1

//  var hardToBuild : zz.test.D.type

//  def apply(implicit sparkSession: SparkSession): HardToBuild1 = hardToBuild(sparkSession)

}
