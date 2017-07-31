package zz.test
import java.sql.Timestamp

import com.google.common.hash.Hashing
import org.apache.spark.sql.SparkSession
import shade.com.datastax.spark.connector.google.common.io.BaseEncoding

import scala.util.Random
import scala.util.hashing.MurmurHash3
/**
 * Hello world!
 *
 */
case class Child(data:String)
case class Parent (name:String, children:Array[Child])
case class Parent2 (name:String,name2:String, children:Array[String])
case class TimeFrame(gte:Timestamp,lte:Timestamp)

case class DateRangeTest(name2:String,name:String,time_frame:TimeFrame)
case class RandomC(word:String)


object App1 extends App {
  val ss = SparkSession.builder().master("local[*]").appName("app")
    .config("es.index.auto.create", "true")
    .config("es.nodes","localhost")
    .config("spark.ui.showConsoleProgress","false")
    .getOrCreate();

//    val a = Seq.range(1,10)
//
//    val m = a.map( i => {
//      val c1= Child(s"aaaa $i")
//      val c2= Child(s"bbbb")
//      Parent(s"name i",Array(c1,c2))
//    })
//
//  val m1 = a.map( i => {
//    val mod = i % 2
//    Parent2(s"name $i",s"mname$mod",Array(s"aaa$i",s"bbb"))
//  })
//

  val nums = 10
  val len = 10
  import ss.implicits._
  import org.apache.spark.sql.functions._

  val toTest = Range(0,nums).map(r=>RandomC(BaseEncoding.base64().encode(Random.nextString(10).getBytes)))
//  println(toTest.size)
  val toTestRDD = ss.sparkContext.parallelize(toTest)
  import com.datastax.spark.connector.rdd.tt._
  toTestRDD.joinWithCassandraTable2("dupa1","words").count()

//  val t1 = toTestRDD.toDF().select(base64($"col").as("col")).cache()
//  val t1 = toTest.toDF("col").select(base64($"col").as("col")).cache()

//  val udf1 = udf((a:String) => MurmurHash3.stringHash(a))
//  val udf2 = udf((a:String) => Hashing.goodFastHash(64).hashString(a).asLong())
//  val udf3 = udf((a:String) => Hashing.goodFastHash(32).hashString(a).asInt())
//  val udf4 = udf((a:String) => Hashing.goodFastHash(256).hashString(a).asBytes())
//  val udf5 = udf((a:String) => Hashing.murmur3_128().hashString(a).asBytes())
//
//  println(s"Hash comparision in spark, number of random strings: $nums with len $len and base64 encoded")
//  println("--")
//  ss.time { p(t1.count(),"Warm") }
//  ss.time { p(t1.select(md5($"col")).count(),"spark_md5") }
//  ss.time { p(t1.select(sha2($"col",256)).count(),"spark_sha2_256") }
//  ss.time { p(t1.select(sha2($"col",512)).count(),"spark_sha2_512") }
//  ss.time { p(t1.select(crc32($"col")).count(),"spark_crc32") }
//  ss.time { p(t1.select(udf1($"col")).count(),"scala_mm3") }
//  ss.time { p(t1.select(udf3($"col")).count(),"guava_goodFastHash_32") }
//  ss.time { p(t1.select(udf2($"col")).count(),"guava_goodFastHash_64") }
//  ss.time { p(t1.select(udf4($"col")).count(),"guava_goodFastHash_256") }
//  ss.time { p(t1.select(udf5($"col")).count(),"guava_murmur_128") }


  def p(c:Long,s:String) = {
    print(s"Alg $s - collisions ${nums-c} -> ")
  }
//  val d = List(
//    Child("a"),
//    Child("b"),
//    Child("c"),
//    Child("d"),
//    Child("e"),
//    Child("f"),
//    Child("g"),
//    Child("h"),
//    Child("i"),
//    Child("j"),
//    Child("k"),
//    Child("l"),
//    Child("n")
//  ).toDS()
//
//  val ac = ss.sparkContext.longAccumulator("dddd")
//
//  d.mapPartitions(f => {
//    val x = f.toStream
//    ac.add(x.length)
//    x.toIterator
//  }).show(100)
//
//
//  Thread.sleep(1000)
//  println(ac.value)
//  println(d.count())

//  val ds = ss.createDataset(m)
//  val ds2 = m1.toDS()
//  val acc = ss.sparkContext.longAccumulator("doc counter")
//
//
//  ds.show()
//  println(ds.count())
//  ds.printSchema()
//  ds.map(d => {
//    acc.add(1L)
//    d}).saveToEs("test/data")
//
//  ds2.saveToEs("test2/data")

//  val tt = Seq(
//    DateRangeTest("name2","hdhdhdhd",TimeFrame(Timestamp.valueOf("2016-01-01 00:12:00"),Timestamp.valueOf("2016-03-01 00:00:00"))),
//    DateRangeTest("name2","xxcssd",TimeFrame(Timestamp.valueOf("2016-03-02 00:00:00"),Timestamp.valueOf("2017-02-01 00:00:00")))
//    ).toDS()
//
//  tt.show()
//
//  tt.saveToEs("range_index/my_type")

//  println(acc.value)

//  val ri = ss.esDF("range_index/my_type")
//
//  ri.printSchema()
//  ri.show()
}
