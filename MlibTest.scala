package zz.test

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Random

object MlibTest extends App {
  val ss = SparkSession.builder().appName("mlib").master("local[*]").getOrCreate()

  import ss.implicits._

  val learnInput = Seq(
    """{"x":1,"a":1,"b":2,"c":"dbteam"}""",
    """{"x":2,"a":0,"b":1,"c":"asteam"}""",
    """{"x":1,"a":9,"b":4,"c":"appteam"}""",
    """{"x":1,"a":9,"b":9,"c":"appteam"}""",
    """{"x":1,"a":9,"b":9,"c":"appteam"}""",
    """{"x":1,"a":9,"b":9,"c":"appteam"}""",
    """{"x":1,"a":9,"b":9,"c":"appteam"}""",
    """{"x":1,"a":9,"b":99,"c":"appteam"}""",
    """{"x":3,"a":8,"b":1,"c":"webteam"}""",
    """{"x":4,"a":7,"b":7,"c":"osteam"}""",
    """{"x":1,"a":9,"b":1,"c":"osteam"}""",
    """{"x":0,"a":0,"b":0,"c":"other"}""",
    """{"x":0,"a":0,"b":0,"c":"other"}""",
    """{"x":0,"a":0,"b":0,"c":"other"}""",
    """{"x":0,"a":0,"b":0,"c":"other"}""",
    """{"x":11,"a":11,"b":-1,"c":"other"}""",
    """{"x":5,"a":99,"b":3,"c":"netteam"}"""
  ).toDS()

  val testInput = Seq(
    """{"x":1,"a":1,"b":2,"c":"dbteam"}""",
    """{"x":2,"a":0,"b":1,"c":"asteam"}""",
    """{"x":2,"a":0,"b":1,"c":"asteam"}""",
    """{"x":2,"a":0,"b":1,"c":"asteam"}""",
    """{"x":2,"a":0,"b":1,"c":"asteam"}""",
    """{"x":2,"a":0,"b":1,"c":"asteam"}""",
    """{"x":1,"a":9,"b":4,"c":"appteam"}""",
    """{"x":3,"a":8,"b":1,"c":"webteam"}""",
    """{"x":5,"a":8,"b":3,"c":"??"}""",
    """{"x":1,"a":7,"b":7,"c":"osteam"}""",
    """{"x":11,"a":1,"b":0,"c":"osteam"}"""
  ).toDS()

  val learnDS = ss.read.json(learnInput)
  val testDS = ss.read.json(testInput)

  val cIndexer = new StringIndexer()
    .setInputCol("c")
    .setOutputCol("label")
    .setHandleInvalid("keep")
    .fit(learnDS)

  val assembler = new VectorAssembler()
    .setInputCols(Array("a", "b"))
    .setOutputCol("raw_features")

  val fIndexer = new VectorIndexer()
    .setInputCol("raw_features")
    .setOutputCol("i_features")

  val assembler2 = new VectorAssembler()
      .setInputCols(Array("x","i_features"))
      .setOutputCol("features")



  val classifier = new DecisionTreeClassifier()
    .setMaxDepth(10)
    .setMaxBins(32)
    .setSeed(Random.nextLong())

  val deIndexer = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("orginal")
    .setLabels(cIndexer.labels)

  val pipe = new Pipeline()
    .setStages(Array(cIndexer, assembler, fIndexer,assembler2, classifier,deIndexer))


  val model = pipe.fit(learnDS)
  val data = model.transform(testDS)


  val isSure = udf { (v: Vector) => v.toArray.contains(1.0) }

  data
    .withColumn("sure", isSure($"probability"))
    .select($"x",$"a",$"b",$"c",$"orginal",$"*")
    .show (false)


  println(model.stages(4).asInstanceOf[DecisionTreeClassificationModel].featureImportances)


}
