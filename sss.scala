package zz.test

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Try}
/**
  * Created by zulk on 17.02.17.
  */
case class Person(date:String,date2:String)
case class S(person: Person,child:Option[Person])

object sss extends App{
  val ss = SparkSession.builder().master("local[*]").appName("app")
    .getOrCreate();

  import ss.implicits._

  val json =Seq(
    S(Person("a","b"),Some(Person("x","y"))),
    S(Person("a","b"),Some(Person("x","y"))),
    S(Person("a","b"),Some(Person("x","y"))),
    S(Person("a","b"),None),
    S(Person("a","b"),Some(Person("x","y")))
    )

  val jsonDF = ss.sparkContext.parallelize(json).toDS()


  jsonDF.show()
  var count = 1

  def isEmpty(d:Dataset[_]): Boolean = {
    import scala.util._
    val result = Try(d.first)
    result match {
      case Success(_) => true
      case Failure(_) => false
    }
  }

  def recurse(dataset: Dataset[S]): Dataset[S] = {
    val cached = dataset.cache()
    val hasChildren = cached.filter(_.child.nonEmpty)

    if(isEmpty(hasChildren)) {
      val done = cached.filter(_.child.isEmpty)
      val children = hasChildren.map(_.child.get).map(a => S(a,None))
      done.union(recurse(children))
    } else {
         cached
    }
  }

  val c = recurse(jsonDF)
  c.show(1000)

}
