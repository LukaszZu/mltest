package zz.test

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.RestClient

/**
  * Created by zulk on 13.06.17.
  */
object MetricsTest extends App{
    val ss = SparkSession.builder().master("local[*]").appName("app")
      .getOrCreate()

    import ss.implicits._
    var outputWritten = 0L
    var inputRecords = 0L


  //  val sc = ss.sparkContext
  //
  //  sc.addSparkListener(new SparkListener() {
  //    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
  //      val metrics = taskEnd.taskMetrics
  //      if(metrics.inputMetrics != None){
  //        inputRecords += metrics.inputMetrics.recordsRead}
  //      if(metrics.outputMetrics != None){
  //        outputWritten += metrics.outputMetrics.recordsWritten }
  //    }
  //  })
  //  val metric = SQLMetrics.createTimingMetric(ss.sparkContext,"dupa")
    val data = ss.range(10000)
    import org.apache.spark.sql.functions._

//  new RestClient()

  val x = data
    .map(l => (l % 2,l)).toDF("key","value")
    .groupBy("key").agg(count($"value"))

  x.show()
//  println(x.queryExecution.executedPlan.collectLeaves())
//  SQLMetrics.postDriverMetricUpdates(ss.sparkContext,"267",List(metric))
//  println(metric.value)
//  println(s"$inputRecords $outputWritten")
}
