package zz.test

import java.sql.Timestamp

import com.datastax.spark.connector.cql.{CassandraConnectorConf, StructDef}
import com.datastax.spark.connector.mapper.{ColumnMapForWriting, DataFrameColumnMapper, SimpleColumnMapForWriting}
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SparkSession}

object ColsStructTest extends App {
  val ss = SparkSession.builder()
    .appName("aliasttets")
    .master("local[*]")
    .getOrCreate()


  import ss.implicits._
  import org.apache.spark.sql.cassandra._
  import com.datastax.spark.connector._
  import org.apache.spark.sql.functions._

  val cols = (1 to 100).map(_.toString)

  val mm = cols.map(c => c -> c).toMap



}
