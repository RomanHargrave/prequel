package net.noerd.prequel

import java.util.{Locale, Date}

import org.joda.time.DateTime
import org.joda.time.Duration

/**
 * Wrap your optional value in NullComparable to compare with null if None.
 *
 * Note: The '=' operator is added during formatting so don't include it in your SQL
 */
class NullComparable(val value: Option[Formattable]) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    value.map("=" + _.escaped(formatter)).getOrElse("is null")
  }

  override def addTo(statement: ReusableStatement): Unit = {
    sys.error("incompatible with prepared statements")
  }
}

object NullComparable {
  def apply(value: Option[Formattable]) = new NullComparable(value)
}

/**
 * Wrap your optional value in Nullable to have it converted to null if None
 */
class Nullable(val value: Option[Formattable]) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    value.map(_.escaped(formatter)).getOrElse("null")
  }

  override def addTo(statement: ReusableStatement): Unit = {
    value match {
      case Some(v) =>
        v.addTo(statement)
      case None =>
        statement.addNull
    }
  }
}

object Nullable {
  def apply(value: Option[Formattable]) = new Nullable(value)
}

/**
 * Wrap a parameter string in an Identifier to avoid escaping
 */
class Identifier(val value: String) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    value
  }

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addString(value)
  }
}

object Identifier {
  def apply(value: String) = new Identifier(value)
}

//
// String
//
class StringFormattable(val value: String) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    formatter.toSQLString(value)
  }

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addString(value)
  }
}

object StringFormattable {
  def apply(value: String) = new StringFormattable(value)
}

//
// Boolean
// 
class BooleanFormattable(val value: Boolean) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = value.toString

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addBoolean(value)
  }
}

object BooleanFormattable {
  def apply(value: Boolean) = new BooleanFormattable(value)
}

//
// Long
//
class LongFormattable(val value: Long) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = value.toString

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addLong(value)
  }
}

object LongFormattable {
  def apply(value: Long) = new LongFormattable(value)
}

//
// Int
//
class IntFormattable(val value: Int) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = value.toString

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addInt(value)
  }
}

object IntFormattable {
  def apply(value: Int) = new IntFormattable(value)
}

//
// Float
//
class FloatFormattable(val value: Float) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = "%f".formatLocal(Locale.US, value)

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addFloat(value)
  }
}

object FloatFormattable {
  def apply(value: Float) = new FloatFormattable(value)
}

//
// Double
//
class DoubleFormattable(val value: Double) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = "%f".formatLocal(Locale.US, value)

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addDouble(value)
  }
}

object DoubleFormattable {
  def apply(value: Double) = new DoubleFormattable(value)
}

//
// DateTime
//
class DateTimeFormattable(val value: DateTime)
  extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    formatter.toSQLString(formatter.timeStampFormatter.print(value))
  }

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addDateTime(value)
  }
}

object DateTimeFormattable {
  def apply(value: DateTime) = {
    new DateTimeFormattable(value)
  }

  def apply(value: Date) = {
    new DateTimeFormattable(new DateTime(value))
  }
}

//
// Date
//
/*class DateFormattable(val value: Date)
  extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    value.getTime.toString
  }

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addDate(new java.sql.Date(value.getTime))
  }
}

object DateFormattable {
  def apply(value: DateTime) = {
    new DateTimeFormattable(value)
  }

  def apply(value: Date) = {
    new DateFormattable(value)
  }
} */

//
// Duration
//
/**
 * Formats an Duration object by converting it to milliseconds.
 */
class DurationFormattable(val value: Duration)
  extends Formattable {
  override def escaped(formatter: SQLFormatter): String = value.getMillis.toString

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addLong(value.getMillis)
  }
}

object DurationFormattable {
  def apply(value: Duration) = new DurationFormattable(value)
}

//
// Binary
//
class BinaryFormattable(val value: Array[Byte]) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = {
    formatter.toSQLString(formatter.binaryFormatter.print(value))
  }

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addBinary(value)
  }
}

object BinaryFormattable {
  def apply(value: Array[Byte]) = new BinaryFormattable(value)
}

//
// Blob
//
class BlobFormattable(val value: java.io.InputStream) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = value.toString

  /*{
      formatter.toSQLString(formatter.binaryFormatter.print(""+value.available()))
    }*/
  override def addTo(statement: ReusableStatement): Unit = {
    CloseUtil.closeAfterUse(value) {
      value =>
        statement.addBlob(value)
    }
  }
}

object BlobFormattable {
  def apply(value: java.io.InputStream) = new BlobFormattable(value)
}


//
// Clob
//
class ClobFormattable(val value: java.io.Reader) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = value.toString

  /*{
      formatter.toSQLString(formatter.binaryFormatter.print(""+value.available()))
    }*/
  override def addTo(statement: ReusableStatement): Unit = {
    CloseUtil.closeAfterUse(value) {
      value =>
        statement.addClob(value)
    }
  }
}

object ClobFormattable {
  def apply(value: java.io.Reader) = new ClobFormattable(value)
}

/*class ListFormattable[A](val list: List[A]) extends Formattable {
  override def escaped(formatter: SQLFormatter): String = list map {
    case s: String => formatter.toSQLString(s)   
    case a: Any         => a

  } mkString(",")

  override def addTo(statement: ReusableStatement): Unit = {
    statement.addString(list map {
      case s: String => SQLFormatter().toSQLString(s)
      case a: Any    => a

    } mkString(","))
    
  }
  
  //def value = list.head
}
object ListFormattable {
  def apply[A](list: List[A]) = new ListFormattable(list)
}*/
