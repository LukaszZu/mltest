package zz.test

import java.util

import kafka.producer.DefaultPartitioner
import org.apache.log4j.LogManager
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Dataset, Encoders, Row, SaveMode, SparkSession}

object JsonTest extends App{

  val ss = SparkSession.builder().master("local").appName("app")
    .config("spark.executor.memory", "1G")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.cassandra.connection.host", "192.168.100.105")
    .getOrCreate();

  ss.listenerManager.register(new QueryExecutionListener {
    val logger = LogManager.getLogger("CustomListener")

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    }

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

      logger.warn(s"${funcName} ${durationNs}")
    }
  })



  class CustomListener extends SparkListener {
    val logger = LogManager.getLogger("CustomListener")
    var acr = 0L;
    var acw = 0L;

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val read = stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead
      val write = stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten
      val time = stageCompleted.stageInfo.taskMetrics.executorRunTime
      val tasks = stageCompleted.stageInfo.numTasks
      val name = stageCompleted.stageInfo.name
      acr+=read
      acw+=write
      logger.warn(s"Stage ${name} completed, read: ${read} write: ${write} time: ${time/1000.0}s tasks: ${tasks}")
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      val time = applicationEnd.time
      logger.warn(s"Application completed, read: ${acr} write: ${acw}")
    }

  }

  val lj = new CustomListener()
  ss.sparkContext.addSparkListener(lj)
  Thread.sleep(50)

  import ss.implicits._

  val dto = new Dto()

  implicit def aaa = Encoders.bean(classOf[Dto])
  val arr = new util.ArrayList[String]()
  arr.add("test")
  arr.add("test2")

  dto.setBar("bar2")
  dto.setFoo("foo")
  dto.setRows(arr.toArray(Array[String]()))

  val ds = Seq(dto,dto).toDS()


  val toSave = ds
    .distinct()
    .toJSON
    .select(lit(classOf[Dto].getSimpleName) as "type",lit("topic.name") as "topic",'value as "data",lit(1) as "retry")

  toSave.distinct().write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "retry_data", "keyspace" -> "keyspace1"))


  val loaded = ss.read.format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "retry_data", "keyspace" -> "keyspace1")).load()

  def tt(s:Dataset[Row]):Dataset[Row] = {
//    s.filter('data.contains("bar1")).show(false)
    s.filter(not('data.contains("bar1")))
  }

  def tt2(s:Dataset[Row]):Dataset[Row] = {
//    s.filter('data.contains("bar2")).show(false)
    s.filter(not('data.contains("bar2")))
  }


//  implicit def rowEnc = RowEncoder(loaded.schema)


  loaded.select('topic.contains("topic")).show()
  loaded.foreach(println(_))

  new DefaultPartitioner
//  StdIn.readLine()
//    transform(tt).transform(tt2).show())

//  j.completedStages.foreach(si => {
//    println(si.name)
//    si.accumulables.map(_._2).foreach( a => {
//      println(a.name.getOrElse("")+a.value.getOrElse(""))
//    }
//    )
//  })
//  loaded.
}
