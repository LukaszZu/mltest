package zz.test

import com.datastax.spark.connector.{ColumnName, ColumnRef}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.writer.{RowWriter, RowWriterFactory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

class RowJoinWriter(val schema: StructType, table: TableDef, val selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[Row] {

  override val columnNames = selectedColumns.map(_.columnName)
  private val aliasedColumns = selectedColumns.map(c => c.asInstanceOf[ColumnName].alias.getOrElse(c.columnName))
  private val columns = columnNames.map(table.columnByName)
  private val converters = columns.map(_.columnType.converterToCassandra)

  /** Extracts column values from Spark SQL Row
    * This extracts only selected column
    * Tested with joinWithCassandraTable */

  override def readColumnValues(row: Row, buffer: Array[Any]) = {

    val getFieldIndex = (i: Int) => schema.fieldIndex(aliasedColumns(i))

    for (i <- aliasedColumns.indices) {
      val colValue = row(getFieldIndex(i))
      buffer(i) = converters(i).convert(colValue)
    }
  }
}

object RowJoinWriter {

  class Factory(val schema: StructType) extends RowWriterFactory[Row] {

    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
      new RowJoinWriter(schema, table, selectedColumns)
  }

}
