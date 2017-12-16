package zz.test


import java.sql.Timestamp

import com.datastax.spark.connector.cql.{CassandraConnectorConf, StructDef}
import com.datastax.spark.connector.mapper.{ColumnMapForWriting, DataFrameColumnMapper, SimpleColumnMapForWriting}
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SparkSession}

case class ToTest(id_name: Int, name: String)

case class ToTest2(id: Int, name: String, name2: String)

object AliasTest extends App {
  val ss = SparkSession.builder()
    .appName("aliasttets")
    .master("local[*]")
    .config(CassandraConnectorConf.ConnectionHostParam.name, "192.168.100.105")
    .config(CassandraConnectorConf.ConnectionPortParam.name, "9042")
    .getOrCreate()


  import ss.implicits._
  import org.apache.spark.sql.cassandra._
  import com.datastax.spark.connector._
  import org.apache.spark.sql.functions._

  val data = ss.range(1, 10)
    .map(_.toInt)
    .toDF("id")
    .withColumn("name", lit("name1"))
    .withColumn("name2", lit("name2"))
    .withColumn("name3", lit("aaa"))
    .withColumn("name4", lit(new Timestamp(1)))
    .withColumn("name5", lit("aaa"))

  //    .as[ToTest2]


  //  data.count()

  //  val uDf = data.select(abs(toUUID($"id").mod(20L)) as "partition", $"id")
  //  uDf.groupBy($"partition").agg(count("*")).show(20)


  //  data.show()
  //    .map(id => A(1, (id % 20), id, "a"))

  //  uDf.write
  //    .cassandraFormat("p_test", "dupa1")
  //    .mode(SaveMode.Append).save()

  class RowColMapper(structType: StructType) extends DataFrameColumnMapper[Row](structType) {
    override def columnMapForWriting(struct: StructDef, selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = {
      val cc = selectedColumns
        .map(c => c.asInstanceOf[ColumnName].alias.getOrElse(c.columnName) -> c).toMap
      println(cc)
      SimpleColumnMapForWriting(cc)
    }
  }

  //  Row().
  //  implicit def mapper = new RowColMapper(data.schema)

//  implicit val rowW = new RowJoinWriter.Factory(data.schema)
  implicit def ee = Encoders.kryo[Map[String,Any]]
  implicit def encoder = Encoders.tuple[Row, ToTest](RowEncoder(data.schema), Encoders.product[ToTest])

  //  implicit val r = new RowColMapper(data.schema)
  data
    .map(_.getValuesMap[Any](data.schema.fieldNames))
    .rdd
      .map(CassandraRow.fromMap)
    .leftJoinWithCassandraTable[ToTest]("test", "to_test2")
    .on(SomeColumns("id_name" as "id", "name" as "name2"))
    .foreach(println)
    //      .where(" ALLOW FILTERING")
//    .map { case (l, r) => (l, r.orNull) }
//    .toDF()
//    .select( $"_2")
//    .show(false)


}
