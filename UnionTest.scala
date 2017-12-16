package zz.test

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.{SaveMode, SparkSession}

case class A(id: Long, skey: Long, cl: Long, col: String)

object UnionTest extends App {

  val ss = SparkSession.builder().appName("mlib").master("local[*]")
    .config(CassandraConnectorConf.ConnectionHostParam.name, "192.168.100.105")
    .config(CassandraConnectorConf.ConnectionPortParam.name, "9042")
    .getOrCreate()


  import ss.implicits._
  import org.apache.spark.sql.cassandra._

  //  ss.range(1,10000000).map(id => A(1,(id % 20),id,"a"))
  //    .write
  //    .cassandraFormat("union_test","dupa1").mode(SaveMode.Append).save()


  private def execOne(p: Int) = {
    val cdf = ss.read.cassandraFormat("union_test", "dupa1").load()
      .filter(($"id" === 1) and ($"skey" === p) and ($"cl" > 100) and ($"col" === "a"))
    cdf
  }

  def execN(j: Int) = {
    var one = execOne(1)
    for (i <- 2 to j) {
      one = one.union(execOne(i))
    }
    one
  }

  val c = execN(10)
  c.explain()
  println(c.count())


  val cdf = ss.read.cassandraFormat("union_test", "dupa1").load()
    .filter(($"id" === 1) and ($"skey" isin ((1 to 10).toArray: _*)) and ($"cl" > 100) and ($"col" === "a"))

  cdf.explain()
  println(cdf.count())

}
