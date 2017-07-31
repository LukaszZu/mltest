package zz.test

import org.apache.spark.sql.SparkSession

/**
  * Created by zulk on 02.06.17.
  */

case class R(id:Int,name:String,data:String)

object MapTest extends App {

  val ss = SparkSession.builder().master("local").appName("app")
    .getOrCreate();

  import ss.implicits._
  import org.apache.spark.sql.functions._


  val ds = List(
    R(1,"name1","data1"),
    R(1,"name1","data2"),
    R(2,"name1","data3"),
    R(2,"name1","data4"),
    R(3,"name1","data5"),
    R(3,"name1","data6"),
    R(4,"name1","data7"),
    R(4,"name1","data8"),
    R(5,"name1","data9"),
    R(5,"name1","data0"),
    R(5,"name1","data8")
  ).toDS()


  val gb = List("id")

  val fn2 = List("X","data")

  val fn = ds.columns.diff(fn2 :: gb)
  import ChainTest._
  def setFirst(c:String) = col(c).chain(first).as(c)
  def collect(c:String) = col(c).chain(collect_set).as(c)
  val aggs = fn.map(setFirst)
  val aggs2 = fn2.map(collect)
//  val aggs2 = fn2.map(c => collect_set(col(c)).as(c))



  val execute = aggs++aggs2
  ds
    .select($"*",struct($"*").as("X"))
    .groupBy("id").agg(execute(0),execute.tail:_*).show(false)

  ds.show()
}
