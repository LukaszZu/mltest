package zz.test

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

case class SomeData(id: Long, data: String, date: Timestamp)

object EsRestTest extends App {

  implicit val ss = SparkSession.builder().master("local").appName("aaa")
    .config("es.nodes", "localhost")
    .getOrCreate()

  import ss.implicits._
  import org.elasticsearch.spark.sql._

  val inc = Seq(
    SomeData(1, "data1", new Timestamp(117, 0, 1, 0, 0, 0, 0)),
    SomeData(2, "data2", new Timestamp(117, 1, 1, 0, 0, 0, 0)),
    SomeData(3, "data3", new Timestamp(117, 2, 1, 0, 0, 0, 0)),
    SomeData(4, "data4", new Timestamp(117, 3, 1, 0, 0, 0, 0)),
    SomeData(5, "data5", new Timestamp(117, 4, 1, 0, 0, 0, 0)),
    SomeData(6, "data6", new Timestamp(117, 5, 1, 0, 0, 0, 0)),
    SomeData(7, "data7", new Timestamp(117, 6, 1, 0, 0, 0, 0)),
    SomeData(8, "data8", new Timestamp(117, 7, 1, 0, 0, 0, 0)),
    SomeData(9, "data9", new Timestamp(117, 8, 1, 0, 0, 0, 0)),
    SomeData(10, "data10", new Timestamp(117, 9, 1, 0, 0, 0, 0))
  ).toDS()


  val assets = (1 to 10).flatMap(a => {
    Seq(
      SomeData(a, "asset1", new Timestamp(16, 1, 1, 0, 0, 0, 0)),
      SomeData(a, "asset2", new Timestamp(16, 1, 2, 0, 0, 0, 0)),
      SomeData(a, "asset3", new Timestamp(16, 1, 3, 0, 0, 0, 0))
    )
  }).toDS()

  val joined = inc.joinWith(assets, inc("id").equalTo(assets("id")))
    .toDF("inc", "asset")
    .select($"inc.id", $"inc.data", $"inc.date", $"asset.data" as "asset_data")

  joined.show()
  joined.saveToEs("colapse/test")

  //  .toDS()


}

case class RankTest(a: Long, b: Long, c: Long)

object rankTest {
  def main(args: Array[String]): Unit = {
    implicit val ss = SparkSession.builder().master("local").appName("aaa")
      .config("es.nodes", "localhost")
      .getOrCreate()

    import ss.implicits._
    import org.apache.spark.sql.functions._

    val win =  Window.partitionBy($"b").orderBy($"c".desc)
    val win1 = Window.partitionBy($"b")

    val ds = ss.range(1,114000000).map(r => RankTest(r, r % 1000, r % 100))




    println(ss.time(ds.sort($"a".desc).first()))
    ss.time(ds.agg(max($"a")).show())
//    val rank = ds.select($"*",
//      row_number().over(win) as "rn",
//      rank().over(win) as "rank",
//      dense_rank().over(win) as "dr"
//    ).filter($"dr".equalTo(1))


//    val rank1 = ds.select($"*",
//      //      row_number().over(win) as "rn",
//      //      rank().over(win) as "rank",
//      max($"c").over(win1) as "dr"
//    ).filter($"dr".equalTo($"c"))
//
//    println(ss.time(rank1.count()))
//    println(ss.time(rank.count()))

//    println(ss.time(rank1.count()))
//    println(ss.time(rank.count()))

//    println(ss.time(rank1.count()))
//    println(ss.time(rank.count()))

//    readLine()
  }
}



