package zz.test

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType}

case class Agg(key: String, doc_count: Long)

case class Field(f: String)


object EstAggregate extends App {


  implicit val v = SparkSession.builder().master("local").appName("eee")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "192.168.100.105")
    .getOrCreate()

  import v.implicits._

  //  val t = new TermQueryBuilder().field("line_id").term("15")
  //  val t1 = new TermQueryBuilder().field("line_id").term("17")
  //  val q = new BoolQueryBuilder().should(t).should(t1)
  //  println(q)

  //  println(SomethingUtils().getAggs("line_id"))


  //  SomethingUtils()(v.sparkContext.getConf).getAggs("play_name")
  //  SomethingUtils()(v.sparkContext.getConf).getAggs("line_id")

  val sc = v.sparkContext.broadcast(v.sparkContext.getConf)
  val dd = Encoders.product[Agg].schema


  val data = Seq(Field("play_name"), Field("line_id"))
    .toDS()
    .map(c =>
      (c.f, SomethingUtils()(sc.value).getAggs(c.f))
    ).toDF()
    .select(from_json($"_2", ArrayType(dd)) as "json", $"_1" as "colName")
    .select($"colName", explode($"json") as "json")
    .select($"colName", $"json.*")

  data.show()

  data.foreachPartition(p => SomethingUtils()(sc.value).close())


  //  print(d)
}
