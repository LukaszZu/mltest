package zz.test

object ObjectTesting {
  def main(args: Array[String]): Unit = {

    implicit val dd = "dupa z implicita"

    (1 to 100)
      .map(String.valueOf)
      .par
      .map(ObjectToTest.doWithS)
      .foreach(println)
  }
}
