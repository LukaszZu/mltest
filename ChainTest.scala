package zz.test

import org.apache.spark.sql.Column

/**
  * Created by zulk on 03.06.17.
  */
object ChainTest {

      implicit class ColumnMethods(col: Column) {

        def chain(colMethod: (Column => Column)): Column = {
          colMethod(col)
        }

      }
}
