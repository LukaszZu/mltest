package zz.test


object ObjectToTest {

  var s: String = ""

  def doWithS(v: String)(implicit dd:String): String = synchronized {
    s = v
    Thread.sleep(10)
    v + ":" + s + "->"+dd
  }

}
