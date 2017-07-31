package zz

import org.apache.spark.sql.Dataset

/**
  * Created by zulk on 29.06.17.
  */
package object test {

    implicit def a[T](d:Dataset[T]) = new Finisher[T](d)

}

class Finisher[U](ds: Dataset[U]) {
    def execute(t: Dataset[U] => Unit): Dataset[U] = {
        t(ds)
        ds
    }
}
