package zz.test

import org.apache.kafka.clients.consumer.{CommitFailedException, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by zulk on 08.06.17.
  */
case class KafkaRecord(key: String, value: String, topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)

object NewKafkaBatchTest extends App {
  val ss = SparkSession.builder().master("local[*]").appName("app")
    .getOrCreate()

  import ss.implicits._

  private val topics = "test_multi,vv"
  val kafkaData = ss.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "192.168.100.105:9093")
    .option("subscribePattern", "test_multi.*")
    .load()
    .select(
      $"key".cast(StringType),
      $"value".cast(StringType),
      $"topic",
      $"partition",
      $"offset",
      $"timestamp",
      $"timestampType")
    .as[KafkaRecord]

  import org.apache.spark.sql.expressions.scalalang.typed
  val kd = kafkaData.groupByKey(_.partition).agg(typed.count(_.offset)).map(f => {
    Thread.sleep(1000)
    f
  })

  val w = kd.writeStream
      .outputMode("update")
    .format("console").start()
//      .foreach(new C).start()

  import collection.JavaConversions._

  val kp = new KafkaProducer[String, String](
    Map("bootstrap.servers" -> "192.168.100.105:9093",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    ))

  implicit val ec = ExecutionContext.global
  val futu = Future({
    val r = new Random()
    var count = 0
    while (count<10) {
      Thread.sleep(2000)
//      println("Executing")
      val p = 1 to 10 map (i => new ProducerRecord[String, String]("test_multi", i.toString, i.toString))
      p.foreach(kp.send(_))
//      println("executed")
      count+=1
    }
  })

  w.sparkSession.streams.addListener(new StreamingQueryListener {
    lazy val consumer = new KafkaConsumer[String,String]( Map(
      "bootstrap.servers" -> "192.168.100.105:9093",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "dupa1",
      "enable.auto.commit" -> "false",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    ))

    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      println(s"--->starting $event")
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      if (event.progress.numInputRows != 0) {
//        println(s"processing ${event.progress.sources(0)}")
        val p = event.progress.sources(0)
        val eo = partitionOffsets(p.endOffset)
        val mapped = eo.map {
          case (tp,o) => (tp,new OffsetAndMetadata(o,""))
        }

        val topicsFound = eo.map {
          case (tp,o) => tp.topic()
        }

          consumer.subscribe(topicsFound)
          consumer.poll(0)
          val partitions = consumer.assignment()
          consumer.pause(partitions)
          print(partitions)
        try {
          consumer.commitSync(mapped)
        } catch {
          case e: CommitFailedException => {
            consumer.commitSync(mapped)
            print(e)
          }
          case e: Throwable => println(e)
        }
      }

    }

    private implicit val formats = Serialization.formats(NoTypeHints)

    def partitionOffsets(str: String): Map[TopicPartition, Long] = {
      try {
        Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
          partOffsets.map { case (part, offset) =>
            new TopicPartition(topic, part) -> offset
          }
        }.toMap
      } catch {
        case NonFatal(x) =>
          throw new IllegalArgumentException(
            s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
      }
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      println(s"terminatedt $event")
      consumer.close()
    }
  })

  w.awaitTermination()

  class C extends ForeachWriter[(Int,Long)] {

    override def process(value: (Int,Long)): Unit = {
      println(value)
//      if ((value.offset % 500) == 0) {
//        throw new RuntimeException("dupa")

//      }
    }

    override def close(errorOrNull: Throwable): Unit = {
      println("close")
    }

    override def open(partitionId: Long, version: Long): Boolean = {
      println("open")
      true
    }
  }

}
