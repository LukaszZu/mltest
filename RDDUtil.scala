package zz.test

import com.datastax.spark.connector.rdd.CassandraJoinRDD
import com.datastax.spark.connector.{AllColumns, ColumnSelector, PartitionKeyColumns}
import org.apache.spark.rdd.RDD
import org.mockito.Mockito
import org.mockito.Mockito._

/**
  * Created by zulk on 11.04.17.
  */
object RDDUtil {

  implicit class R[T](rdd: RDD[T]) {

    def joinWithCassandraTable[R](
                                   keyspaceName: String, tableName: String,
                                   selectedColumns: ColumnSelector = AllColumns,
                                   joinColumns: ColumnSelector = PartitionKeyColumns): CassandraJoinRDD[T,R] = {

      val m = mock(classOf[CassandraJoinRDD[T,R]])
      when(m.count()).thenReturn(100L)
      when(m.cassandraCount()).thenReturn(100L)
      null
//      new CassandraJoinRDD[T,R]()
    }
  }

}
