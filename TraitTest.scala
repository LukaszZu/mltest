package zz.test

/**
  * Created by zulk on 31.05.17.
  */
abstract class TraitTest {

  def a:String

  def doSomething(): String = {
    a
  }
}

object TraitTest {

  def apply(aa:String): TraitTest = new TraitTest() {
    override def a: String = aa
  }
}
