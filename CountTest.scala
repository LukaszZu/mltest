package zz.test

import com.datastax.spark.connector.rdd.tt
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
  * Created by zulk on 20.03.17.
  */

case class WithDate(word:String,date2:java.sql.Timestamp)
object CountTest extends App {

  /*
    * @startuml
    *  partition a {
    *   (*)-->[kable]Start
    *   if "cos" then
    *    -->h
    *    -->[dupa] x
    *
    *   else
    *    -->(*)
    *   endif
    *   -->Start
    *   }
    *   partition b {
    *
    *   }
    * @enduml
  */

  /** @param block
    * @tparam R
    * @return
    */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  val ss = SparkSession.builder().master("spark://192.168.100.105:7077").appName("app")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "localhost")
    .config("spark.executor.memory", "2G")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.cassandra.connection.host", "192.168.100.105")
    .getOrCreate();



//  CassandraConnector(ss.sparkContext).withClusterDo(c => {
//    val b = QueryLogger.builder()
//      .withConstantThreshold(1000)
//      .build()
//    c.register(b)
//    c.getConfiguration().getQueryOptions().setDefaultIdempotence(true);
//    c.getConfiguration.getPoolingOptions
//      .setMaxRequestsPerConnection(HostDistance.REMOTE,1024)
//      .setMaxRequestsPerConnection(HostDistance.LOCAL,1024)
//  })


//  CassandraConnector(ss.sparkContext).withClusterDo(c => {
//    println(c.getConfiguration.getQueryOptions.getDefaultIdempotence)
//  })

  //TODO a a a
  import ss.implicits._

  val json =
    """{"date":"$f","date2":1490853772403}""".stripMargin

  val a = s"ss"




  println(json)
  val seq = Seq.tabulate[String](1)(f => s"""{"word":"$f","date2":1490853772403}""")
  val rdd = ss.sparkContext.parallelize(seq)

  import com.datastax.spark.connector.rdd.tt._

  rdd.joinWithCassandraTable2("a","b").toDF().count()


  val df = ss.read.schema(
    StructType(Seq(
      StructField("word", StringType),
      StructField("date2", LongType)
    ))
  ).json(rdd).cache()

//  import com.datastax.spark.connector.cql.CassandraConnector
    import com.datastax.spark.connector._
//  CassandraConnector(ss.sparkContext.getConf).withSessionDo { session =>
//    session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
//    session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, date2 timestamp)")
//  }

  val df1 = ss.read.format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "words", "keyspace" -> "test2" )).load().repartition($"word").sortWithinPartitions(s"word").persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  val ud = udf(Helpers.convertToDate(_:String))

//  df1.show()

  val x = df.select($"word",ud($"date2").as("date2"))
//    .repartition($"word").sortWithinPartitions(s"word").cache()

//  x.show(false)
    df.groupBy('word).agg(count('date1)).show()


//  df.as[WithDate].rdd.joinWithCassandraTable("test2","words",AllColumns).foreach(println(_))
//  c.show()
//  val c1 = x.join(df1,x("word").equalTo(df1("word"))).count()
//  println(c1)

//  x.write.mode(SaveMode.Overwrite).format("org.apache.spark.sql.cassandra")
//    .options(Map( "table" -> "words", "keyspace" -> "test2" )).save()

  //  ss.createDataset(s)(Encoders.LONG).show()
  //  val df = ss.range(10000000)
  //  s.toDS().show()
  //  df.explain(true)
  //  df1.explain(true)
  //  time { val vc = df.count() }
    readLine()
  //  println(vc)

  //  val vc2 = df.select(approx_count_distinct($"value")).show()
}
