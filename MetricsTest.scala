package zz.test

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, internal}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.util.QueryExecutionListener
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.RestClient

/**
  * Created by zulk on 13.06.17.
  */
object MetricsTest extends App {
  val ss = SparkSession.builder().master("local[*]").appName("app")
    .config("spark.metrics.namespace", "aaaaa")
    //    .config("spark.metrics.conf", "/tmp/aa.properties")
    .getOrCreate()

  import ss.implicits._

  var outputWritten = 0L
  var inputRecords = 0L


  val sc = ss.sparkContext
  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 100) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 1) {
      return "%.1f s".format(seconds)
    }
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    "%.1f h".format(hours)
  }

  val sqlll = new  SQLListener(sc.getConf)
  sc.addSparkListener(sqlll)

  sc.addSparkListener(new SparkListener() {
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
//      val metrics = taskEnd.taskMetrics
//      if (metrics.inputMetrics != None) {
//        inputRecords += metrics.inputMetrics.recordsRead
//      }
//      if (metrics.outputMetrics != None) {
//        outputWritten += metrics.outputMetrics.recordsWritten
//      }
//      println(taskEnd.taskInfo.accumulables.foreach(a => println(s"${a.name} ${a.value}")))
//      println(s"${outputWritten}  ${inputRecords}")

    }

    def minusTwoOptions(submissionTime: Option[Long], completionTime: Option[Long]):Long = {
      completionTime.getOrElse(0L) - submissionTime.getOrElse(0L)
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      println()
      val tm = stageCompleted.stageInfo.taskMetrics
      val si = stageCompleted.stageInfo
//      println(
//        s"Read: ${tm.inputMetrics.recordsRead} -> " +
//        s"Write: ${tm.outputMetrics.recordsWritten} -> " +
//        s"Num tasks: ${si.numTasks} " +
//        s"Num tasks: ${si.name} " +
//        s"Num tasks: ${minusTwoOptions(si.submissionTime,si.completionTime)} " +
//          s"")
//      stageCompleted.stageInfo.accumulables
//        .values
////          .map(a => {println(a.name); a})
//        .filter(_.name.contains("internal.metrics.input.recordsRead"))
//        .foreach(a=> println(s"${stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead} ${a.name.get} -> ${a.value.getOrElse(0)}"))
//      stageCompleted.stageInfo.accumulables.foreach(println)
    }
  })

//  ss.listenerManager.register(new QueryExecutionListener {
//    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
//
//    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
//
//      println(qe.executedPlan.children.foreach(s => s.))
//      println("ddd")
//    }
//  })
  //  val metric = SQLMetrics.createTimingMetric(ss.sparkContext,"dupa")
  val data = ss.range(10000000)

  import org.apache.spark.sql.functions._


  implicit def aa = RowEncoder(data.schema)

  val m = SQLMetrics.createMetric(ss.sparkContext, "metricToTest")
  //  new RestClient()


  val x = data
    .map(l => (l % 2, l)).toDF("key", "value")
    .sort($"value")
    .groupBy("key").agg(count($"value"))

  //  println(x.quer

  x.show()
  println(sqlll)
//  readLine()
  //  println(x.queryExecution.executedPlan.collectLeaves())
  //  SQLMetrics.postDriverMetricUpdates(ss.sparkContext,"267",List(metric))
  //  println(metric.value)
  //  println(s"$inputRecords $outputWritten")
}
