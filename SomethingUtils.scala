package zz.test


import java.beans.Transient
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.rest.Request.Method._
import org.elasticsearch.hadoop.rest._
import org.elasticsearch.hadoop.rest.query.{QueryBuilder, TermQueryBuilder}
import org.elasticsearch.hadoop.serialization.json.{JacksonJsonGenerator, JsonFactory}
import org.elasticsearch.hadoop.util.{BytesArray, FastByteArrayOutputStream}
import org.elasticsearch.spark.cfg.SparkSettingsManager

import scala.io.Source

class SomethingUtils(sparkConf: SparkConf) extends Serializable {
  var i = 0

  import SomethingUtils._

  @transient lazy val sm = {
    val sm = new SparkSettingsManager().load(sparkConf)
    new NetworkClient(sm)
  }

  lazy val om = new ObjectMapper()


  def exec() = {
    i += 1
    println(i)
  }

  def executeAndGet(indexName: String, q: QueryBuilder): Response = {
    val response = sm.execute(new SimpleRequest(GET, null, s"$indexName/_search", searchRequest(q)))
    response
  }

  def aggregateAndGet(indexName: String, q: String): Response = {
    val response = sm.execute(new SimpleRequest(GET, null, s"$indexName/_search", searchRequest(q)))
    response
  }

  def getAggs(fieldName: String): String = {
    val template =
      s"""
         |{
         |  "size":0,
         |  "query": {
         |    "match_all": {}
         |  },
         |  "aggs": {
         |    "1": {
         |      "terms": {
         |        "field": "$fieldName"
         |      }
         |    }
         |  }
         |}
    """.stripMargin
    val resource = aggregateAndGet("shakespeare", template)
    val s = om.readTree(resource.body())
    //  println(s)
    val aggs = s.get("aggregations").get("1").get("buckets")
    aggs.toString
  }

  def execute(indexName: String, q: QueryBuilder) = {

    val response = sm.execute(new SimpleRequest(POST, null, s"$indexName/_delete_by_query", searchRequest(q)))

    if (response.isClientError)
      println("Client error")
    if (response.isSuccess) {
      val map = om.readValue(response.body(), classOf[java.util.Map[String, Any]])
      println(map)
    } else {
      val map = om.readValue(response.body(), classOf[java.util.Map[String, Any]])
      val failure = Option(map.get("failures").asInstanceOf[util.ArrayList[String]])
      throw new RuntimeException(failure.getOrElse("").toString)
    }

  }

  def close(): Unit = {
    sm.close()
  }

  def searchRequest(query: String) = {
    val out = new FastByteArrayOutputStream(new BytesArray(query.getBytes))
    out.bytes
  }


  def searchRequest(query: QueryBuilder) = {
    val out = new FastByteArrayOutputStream(256)
    val generator = new JacksonJsonGenerator(out)
    try {
      generator.writeBeginObject
      generator.writeFieldName("query")
      generator.writeBeginObject
      query.toJson(generator)
      generator.writeEndObject
      generator.writeEndObject
    } finally generator.close()
    out.bytes
  }
}


object SomethingUtils {
  @transient private var instance: SomethingUtils = _

  def apply()(implicit sparkConf: SparkConf): SomethingUtils = {
    if (instance == null)
      instance = new SomethingUtils(sparkConf)
    instance
  }
}


object Go {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local").appName("aaa")
      .config("es.nodes", "localhost")
      .getOrCreate()

    implicit val sc = ss.sparkContext.getConf
    val t = new TermQueryBuilder().field("line_id").term("3276")
    SomethingUtils().execute("shakespeare", t)
    SomethingUtils().execute("shakespeare", t)
    SomethingUtils().close()
  }
}
