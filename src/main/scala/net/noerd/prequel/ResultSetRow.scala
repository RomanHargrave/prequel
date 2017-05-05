package net.noerd.prequel

import java.util.Date

import java.sql.{Time, ResultSetMetaData, ResultSet}

import scala.collection.mutable.ArrayBuffer


/**
 * Wraps a ResultSet in a row context. The ResultSetRow gives access
 * to the current row with no possibility to change row. The data of
 * the row can be accessed though the next<Type> methods which return
 * the optional value of the next column, or through its column name.
 */
class ResultSetRow(val rs: ResultSet) {

  private val text = ArrayBuffer[String]()

  lazy val rowValues = "{" + text.mkString(",") + "}"

  /** Maintain the current position. */
  private var position = 0

  def nextBoolean: Option[Boolean] = nextValueOption(rs.getBoolean)

  def nextInt: Option[Int] = nextValueOption(rs.getInt)

  def nextLong: Option[Long] = nextValueOption(rs.getLong)

  def nextFloat: Option[Float] = nextValueOption(rs.getFloat)

  def nextDouble: Option[Double] = nextValueOption(rs.getDouble)

  def nextString: Option[String] = nextValueOption(rs.getString)

  def nextDate: Option[Date] = nextValueOption(rs.getTimestamp)

  def nextObject: Option[AnyRef] = nextValueOption(rs.getObject)

  def nextBinary: Option[Array[Byte]] = nextValueOption(rs.getBytes)

  def nextArray: Option[java.sql.Array] = nextValueOption(rs.getArray)

  def nextBlob: Option[java.io.InputStream] = nextValueOption(rs.getBinaryStream)

  def nextClob: Option[java.io.Reader] = nextValueOption(rs.getCharacterStream)

  def columnBoolean(columnName: String): Option[ Boolean ] = columnValueOption(columnName, rs.getBoolean )

  def columnInt(columnName: String): Option[ Int ] = columnValueOption(columnName, rs.getInt )

  def columnLong(columnName: String): Option[ Long ] = columnValueOption(columnName, rs.getLong )

  def columnFloat(columnName: String): Option[ Float ] = columnValueOption(columnName, rs.getFloat )

  def columnDouble(columnName: String): Option[ Double ] = columnValueOption(columnName, rs.getDouble )

  def columnString(columnName: String): Option[ String ] = columnValueOption(columnName, rs.getString )

  def columnDate(columnName: String): Option[ Date ] =  columnValueOption(columnName, rs.getTimestamp )

  def columnObject(columnName: String): Option[ AnyRef ] = columnValueOption(columnName, rs.getObject )

  def columnBinary(columnName: String): Option[Array[Byte]] = columnValueOption(columnName, rs.getBytes)

  def columnArray(columnName: String): Option[java.sql.Array] = columnValueOption(columnName, rs.getArray)

  def columnBlob(columnName: String): Option[java.io.InputStream] = columnValueOption(columnName, rs.getBinaryStream)

  def columnClob(columnName: String): Option[java.io.Reader] = columnValueOption(columnName, rs.getCharacterStream)


  val columnNames: ArrayBuffer[String] = {
    val columnNames = ArrayBuffer.empty[String]
    val metaData = rs.getMetaData
    for (index <- 0.until(metaData.getColumnCount)) {
      columnNames += metaData.getColumnName(index + 1).toLowerCase
    }
    columnNames
  }

  private def incrementPosition = {
    position = position + 1
  }

  private def nextValueOption[T](f: (Int) => T): Option[T] = {
    incrementPosition
    val value = f(position)
    if (rs.wasNull) {
      None
    }else{
      Some(value)
    }
  }

  def columnValueOption[T](columnName:String, f: (String) => T ): Option[ T ] = {
    val value = f(columnName)
    if( rs.wasNull ) None
    else Some( value )
  }

  def getRowValues = {
    val s: StringBuffer = new StringBuffer(" {")
    val md: ResultSetMetaData = rs.getMetaData
    if (md.getColumnCount > 0){
      s.append(columnNames(0) + ": " + rs.getObject(1))
      (2 to md.getColumnCount) foreach { i =>
        s.append(", ").append(columnNames(i - 1) + ": " + rs.getObject(i))
      }
    }
    s.append("}").toString
  }
}

object ResultSetRow {

  def apply(rs: ResultSet): ResultSetRow = {
    new ResultSetRow(rs)
  }

}

case class Column(name: String)

/**
 * Defines a number of implicit conversion methods for the supported ColumnTypes. A call
 * to one of these methods will return the next value of the right type. The methods make
 * it easy to step through a row in order to build an object from it as shown in the example
 * below.
 *
 * Handles all types supported by Prequel as well as Option variants of those.
 *
 * import net.noerd.prequel.ResultSetRowImplicits._
 *
 * case class Person( id: Long, name: String, birthdate: DateTime )
 *
 * InTransaction { tx =>
 * tx.select( "select id, name, birthdate from people" ) { r =>
 * Person( r, r, r )
 * }
 * }
 */
object ResultSetRowImplicits {
  implicit def row2Boolean(row: ResultSetRow) = BooleanColumnType(row).nextValue

  implicit def row2Int(row: ResultSetRow): Int = IntColumnType(row).nextValue

  implicit def row2Long(row: ResultSetRow): Long = LongColumnType(row).nextValue

  implicit def row2Float(row: ResultSetRow) = FloatColumnType(row).nextValue

  implicit def row2Double(row: ResultSetRow) = DoubleColumnType(row).nextValue

  implicit def row2String(row: ResultSetRow) = StringColumnType(row).nextValue

  //implicit def row2Date(row: ResultSetRow) = DateTimeColumnType(row).nextValue
  implicit def row2Date(row: ResultSetRow) = DateColumnType(row).nextValue

  implicit def row2DateTime(row: ResultSetRow) = DateTimeColumnType(row).nextValue

  implicit def row2Duration(row: ResultSetRow) = DurationColumnType(row).nextValue

  implicit def row2Binary(row: ResultSetRow) = BinaryColumnType(row).nextValue

  implicit def row2InputStream(row: ResultSetRow) = BlobColumnType(row).nextValue

  implicit def row2CharacterInputStream(row: ResultSetRow) = ClobColumnType(row).nextValue


  implicit def row2BooleanOption(row: ResultSetRow) = BooleanColumnType(row).nextValueOption

  implicit def row2IntOption(row: ResultSetRow) = IntColumnType(row).nextValueOption

  implicit def row2LongOption(row: ResultSetRow) = LongColumnType(row).nextValueOption

  implicit def row2FloatOption(row: ResultSetRow) = FloatColumnType(row).nextValueOption

  implicit def row2DoubleOption(row: ResultSetRow) = DoubleColumnType(row).nextValueOption

  implicit def row2StringOption(row: ResultSetRow) = StringColumnType(row).nextValueOption

  implicit def row2DateOption(row: ResultSetRow) = DateColumnType(row).nextValueOption

  implicit def row2DateTimeOption(row: ResultSetRow) = DateTimeColumnType(row).nextValueOption

  implicit def row2DurationOption(row: ResultSetRow) = DurationColumnType(row).nextValueOption

  implicit def row2BinaryOption(row: ResultSetRow) = BinaryColumnType(row).nextValueOption

  implicit def row2InputStreamOption(row: ResultSetRow) = BlobColumnType(row).nextValueOption

  implicit def row2CharacterInputStreamOption(row: ResultSetRow) = ClobColumnType(row).nextValueOption

  // Conversions for basic primives from SQL Arrays to Java Arrays
  implicit def row2StringArrayOpt(row: ResultSetRow) = ArrayColumnType[String](row).nextValueOption
  implicit def row2StringArray(row: ResultSetRow) = ArrayColumnType[String](row).nextValue
  implicit def row2ShortArrayOpt(row: ResultSetRow) = ArrayColumnType[Short](row).nextValueOption
  implicit def row2ShortArray(row: ResultSetRow) = ArrayColumnType[Short](row).nextValue
  implicit def row2IntArrayOpt(row: ResultSetRow) = ArrayColumnType[Int](row).nextValueOption
  implicit def row2IntArray(row: ResultSetRow) = ArrayColumnType[Int](row).nextValue
  implicit def row2LongArrayOpt(row: ResultSetRow) = ArrayColumnType[Long](row).nextValueOption
  implicit def row2LongArray(row: ResultSetRow) = ArrayColumnType[Long](row).nextValue
  implicit def row2FloatArrayOpt(row: ResultSetRow) = ArrayColumnType[Float](row).nextValueOption
  implicit def row2FloatArray(row: ResultSetRow) = ArrayColumnType[Float](row).nextValue
  implicit def row2DoubleArrayOpt(row: ResultSetRow) = ArrayColumnType[Double](row).nextValueOption
  implicit def row2DoubleArray(row: ResultSetRow) = ArrayColumnType[Double](row).nextValue
  implicit def row2DateArrayOpt(row: ResultSetRow) = ArrayColumnType[Date](row).nextValueOption
  implicit def row2DateArray(row: ResultSetRow) = ArrayColumnType[Date](row).nextValue
  implicit def row2BooleanArrayOpt(row: ResultSetRow) = ArrayColumnType[Boolean](row).nextValueOption
  implicit def row2BooleanArray(row: ResultSetRow) = ArrayColumnType[Boolean](row).nextValue
}

object ResultSetRowColumnImplicits {
  implicit def row2Boolean( column: Column )(implicit row: ResultSetRow) = BooleanColumnType( row ).columnValue(column.name)
  implicit def row2Int( column: Column )(implicit row: ResultSetRow): Int = IntColumnType( row ).columnValue(column.name)
  implicit def row2Long( column: Column )(implicit row: ResultSetRow): Long = LongColumnType( row ).columnValue(column.name)
  implicit def row2Float( column: Column )(implicit row: ResultSetRow) = FloatColumnType( row ).columnValue(column.name)
  implicit def row2Double( column: Column )(implicit row: ResultSetRow) = DoubleColumnType( row ).columnValue(column.name)

  implicit def row2String( column: Column )(implicit row: ResultSetRow) = StringColumnType( row ).columnValue(column.name)
  implicit def row2Date( column: Column )(implicit row: ResultSetRow) = DateColumnType( row ).columnValue(column.name)
  implicit def row2DateTime( column: Column )(implicit row: ResultSetRow) = DateTimeColumnType( row ).columnValue(column.name)

  implicit def row2Duration( column: Column )(implicit row: ResultSetRow) = DurationColumnType( row ).columnValue(column.name)

  implicit def row2BooleanOption( column: Column )(implicit row: ResultSetRow) = BooleanColumnType( row ).columnValueOption(column.name)
  implicit def row2IntOption( column: Column )(implicit row: ResultSetRow) = IntColumnType( row ).columnValueOption(column.name)
  implicit def row2LongOption( column: Column )(implicit row: ResultSetRow) = LongColumnType( row ).columnValueOption(column.name)
  implicit def row2FloatOption( column: Column )(implicit row: ResultSetRow) = FloatColumnType( row ).columnValueOption(column.name)
  implicit def row2DoubleOption( column: Column )(implicit row: ResultSetRow) = DoubleColumnType( row ).columnValueOption(column.name)

  implicit def row2StringOption( column: Column )(implicit row: ResultSetRow) = StringColumnType( row ).columnValueOption(column.name)
  implicit def row2DateOption( column: Column )(implicit row: ResultSetRow) = DateColumnType( row ).columnValueOption(column.name)
  implicit def row2DateTimeOption( column: Column )(implicit row: ResultSetRow) = DateTimeColumnType( row ).columnValueOption(column.name)
  implicit def row2DurationOption( column: Column )(implicit row: ResultSetRow) = DurationColumnType( row ).columnValueOption(column.name)
}
