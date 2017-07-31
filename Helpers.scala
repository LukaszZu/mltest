package zz.test

import java.sql.Timestamp

/**
  * Created by zulk on 30.03.17.
  */
object Helpers {
  implicit class ToDateTime(millis: Long) { def toTimestamp = new Timestamp(millis) }

  def convertToDate(s:String) = (Option(s)) match {
    case Some(x) => x.toLong.toTimestamp
    case _ => null
  }

  def convertToDate2(s:String) =  s.toLong.toTimestamp

}
