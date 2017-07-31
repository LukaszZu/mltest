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
case class RandomC2(col:String)


object App12 extends App {
  val ss = SparkSession.builder().master("local[*]").appName("app")
    .config("es.index.auto.create", "true")
    .config("es.nodes","localhost")
    .config("spark.ui.showConsoleProgress","false")
    .getOrCreate();


  val nums = 1000000
  val len = 10
  import ss.implicits._
  import org.apache.spark.sql.functions._

  val toTest = Range(0,nums).map(r=>RandomC2(BaseEncoding.base64().encode(Random.nextString(10).getBytes)))
//  println(toTest.size)
  val toTestRDD = ss.sparkContext.parallelize(toTest)

  val t1 = toTestRDD.toDF().select(base64($"col").as("col")).cache()

  val udf1 = udf((a:String) => MurmurHash3.stringHash(a))
//  val udf2 = udf((a:String) => Hashing.goodFastHash(64).hashString(a).asLong())
//  val udf3 = udf((a:String) => Hashing.goodFastHash(32).hashString(a).asInt())
//  val udf4 = udf((a:String) => Hashing.goodFastHash(256).hashString(a).asBytes())
//  val udf5 = udf((a:String) => Hashing.murmur3_128().hashString(a).asBytes())

  println(s"Hash comparision in spark, number of random strings: $nums with len $len and base64 encoded")
  println("--")
  ss.time { p(t1.count(),"Warm") }
  ss.time { p(t1.select(md5($"col")).distinct().count(),"spark_md5") }
  ss.time { p(t1.select(sha2($"col",256)).distinct().count(),"spark_sha2_256") }
  ss.time { p(t1.select(sha2($"col",512)).distinct().count(),"spark_sha2_512") }
  ss.time { p(t1.select(crc32($"col")).distinct().count(),"spark_crc32") }
  ss.time { p(t1.select(hash($"col")).distinct().count(),"spark_hash") }
  ss.time { p(t1.select(udf1($"col")).distinct().count(),"scala_mm3") }
//  ss.time { p(t1.select(udf3($"col")).distinct().count(),"guava_goodFastHash_32") }
//  ss.time { p(t1.select(udf2($"col")).distinct().count(),"guava_goodFastHash_64") }
//  ss.time { p(t1.select(udf4($"col")).distinct().count(),"guava_goodFastHash_256") }
//  ss.time { p(t1.select(udf5($"col")).distinct().count(),"guava_murmur_128") }


  def p(c:Long,s:String) = {
    print(s"Alg $s - collisions ${nums-c} -> ")
  }
}
