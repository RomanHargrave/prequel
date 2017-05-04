package net.noerd.prequel

import org.joda.time.{DateTime, Duration}
import org.scalatest.FunSpec
import org.scalatest.Matchers
import java.util.{Date, Locale}

class FormattablesSpec extends FunSpec with Matchers {

  val formatter = SQLFormatter.HSQLDBSQLFormatter
  val formattables = List(
    ("StringFormattable should escape foo",
      StringFormattable("foo"), "'foo'"
      ),
    ("LongFormattable should escape 1234",
      LongFormattable(1234L), "1234"
      ),
    ("BooleanFormattable should escape true",
      BooleanFormattable(true), "true"
      ),
    ("BooleanFormattable should escape false",
      BooleanFormattable(false), "false"
      ),
    ("FloatFormattable should escape 1.500000",
      FloatFormattable(1.500000F), "1.500000"
      ),
    ("DoubleFormattable should escape 1.500000",
      DoubleFormattable(1.500000F), "1.500000"
      ),
    ("DateTimeFormattable should escape 2010-03-13 13:00:00.0000",
      DateTimeFormattable(formatter.timeStampFormatter.parseDateTime("2010-03-13 13:00:00.0000")),
      "'2010-03-13 13:00:00.0000'"
      ),
    ("DurationFormattable should escape an Duration object",
      DurationFormattable(Duration.standardHours(2)), "7200000"
      ),
    ("NullComparable should escape defined formattable",
      NullComparable(Some(StringFormattable("foo"))), "='foo'"
      ),
    ("NullComparable should escape undefined formattable",
      NullComparable(None), "is null"
      ),
    ("Nullable should escape defined formattables",
      Nullable(Some(StringFormattable("foo"))), "'foo'"
      ),
    ("Nullable should escape undefined formattables",
      Nullable(None), "null"
      ),
    ("Identifier should leave string as they are",
      Identifier("this.is.a.stri'ng"), "this.is.a.stri'ng"
      ),
    ("BinaryFormattable should escape and convert defined formattable to hexadecimal format",
      BinaryFormattable("Lorem Ipsum".getBytes),
      "'4C6F72656D20497073756D'"
      ),
    ("ListFormattable should escape 'foo','bar','baz' as IN parameters",
      ListFormattable(List("foo", "bar", "baz")), "'foo','bar','baz'"
      ),
    ("ListFormattable should escape 1,2,3 as IN parameters",
      ListFormattable(List(1,2,3)), "1,2,3"
      ),
    ("ListFormattable should escape '2016-08-27 18:39:00.0000' as IN parameters",
      ListFormattable(List(new DateTime(2016, 8, 27, 18, 39))), "'2016-08-27 18:39:00.0000'"
      ),
    ("ListFormattable should escape '2016-08-28 18:39:00.0000' as IN parameters",
      ListFormattable(List(new Date(116, 7, 28, 18, 39))), "'2016-08-28 18:39:00.0000'"
      ),
    ("ListFormattable should escape 1.0,2.0,3.0 as IN parameters",
      ListFormattable(List(1.0,2.0,3.0)), "1.0,2.0,3.0"
      )
  )

  describe("Formattables") {

    formattables.foreach {
      case (description, formattable, expected) =>
        it(description) {
          formattable.escaped(formatter) should equal(expected)
        }
    }
  }

  describe("FloatFormattable") {
    it("should escape 1.5 correctly even with non-US locales") {
      val oldDefaultLocale = Locale.getDefault

      Locale.setDefault(Locale.GERMANY)
      FloatFormattable(1.500000f).escaped(formatter) should equal("1.500000")
      Locale.setDefault(oldDefaultLocale)
    }
  }

  describe("DoubleFormattable") {
    it("should escape 1.5 correctly even with non-US locales") {
      val oldDefaultLocale = Locale.getDefault

      Locale.setDefault(Locale.GERMANY)
      DoubleFormattable(1.500000f).escaped(formatter) should equal("1.500000")
      Locale.setDefault(oldDefaultLocale)
    }
  }
}