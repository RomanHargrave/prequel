package net.noerd.prequel

import java.sql.{SQLException, Types}

import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach
import net.noerd.prequel.SQLFormatterImplicits._
import net.noerd.prequel.ResultSetRowImplicits._

class TransactionSpec extends FunSpec with Matchers with BeforeAndAfterEach {

  val database = TestDatabase.config

  override def beforeEach() = database.transaction {
    tx =>
      tx.execute("create table transactionspec(id int, name varchar(265))")
      tx.execute("insert into transactionspec values(?, ?)", 242, "test1")
      tx.execute("insert into transactionspec values(?, ?)", 23, "test2")
      tx.execute("insert into transactionspec values(?, ?)", 42, "test3")
      tx.execute("CREATE PROCEDURE\n " +
        "new_transactionspec(IN id int, \n" +
        "IN name VARCHAR(265), OUT temp_id1 INTEGER)     MODIFIES SQL DATA\n" +
        "   BEGIN ATOMIC\n     " +
        "DECLARE temp_id INTEGER;\n     " +
        "INSERT INTO transactionspec " +
        "VALUES " +
        "(12345, name);\n     " +
        "SET temp_id = IDENTITY();\n     " +
        "SET temp_id1 = 12345 ;\n" +
        "END")
      tx.execute("CREATE FUNCTION an_hour_before(t TIMESTAMP)\n  RETURNS TIMESTAMP\n  RETURN t - 1 HOUR")
  }

  override def afterEach() = database.transaction {
    tx =>
      tx.execute("drop function an_hour_before")
      tx.execute("drop procedure new_transactionspec")
      tx.execute("drop table transactionspec")

  }

  describe("Transaction") {

    describe("select") {

      it("should return a Seq of the records converted by block") {
        database.transaction {
          tx =>
            val expected = Seq("test1", "test2", "test3")
            val actual = Database(tx.connection).select("select name from transactionspec") {
              row =>
                row.nextString.get
            }

            actual should equal(expected)
        }
      }

      it("should return an empty Seq if no records were found") {
        database.transaction {
          tx =>
            val actual = Database(tx.connection).select(
              """select name from transactionspec
                            where id > 1000
              """
            ) {
              _.nextString.get
            }

            actual should be('empty)
        }
      }
    }

    describe("selectHeadOption") {

      it("should return the first record if one or more is returned by query") {
        database.transaction {
          tx =>
            val expected = Some("test1")
            val actual = Database(tx.connection).selectHeadOption("select name from transactionspec") {
              row =>
                row.nextString.get
            }

            actual should equal(expected)
        }
      }

      it("should return None if the query did not return any records") {
        database.transaction {
          tx =>
            val expected = None
            val actual = Database(tx.connection).selectHeadOption(
              """select name from transactionspec
                            where id > 1000
              """
            ) {
              _.nextString.get
            }

            actual should equal(expected)
        }
      }
    }

    describe("selectHead") {

      it("should return the first column of the first record") {
        database.transaction {
          tx =>
            val expected = 242L
            val actual = Database(tx.connection).selectHead("select id from transactionspec")(row2Long)

            actual should equal(expected)
        }
      }

      it("should throw a NoSuchElementException if no record was returned") {
        database.transaction {
          tx =>
            intercept[NoSuchElementException] {
              Database(tx.connection).selectHead(
                """select id from transactionspec
                                where id > 1000
                """
              )(row2Long)
            }
        }
      }
    }

    describe("selectLong") {

      it("should return the first column of the first records as a Long") {
        database.transaction {
          tx =>
            val expected = 242L
            val actual = Database(tx.connection).selectLong("select id from transactionspec")

            actual should equal(expected)
        }
      }

      it("should throw an SQLException if the value is not a Long") {
        database.transaction {
          tx =>
            intercept[SQLException] {
              Database(tx.connection).selectLong("select 'nan' from transactionspec")
            }
        }
      }

      it("should throw a NoSuchElementException if no record was returned") {
        database.transaction {
          tx =>
            intercept[NoSuchElementException] {
              Database(tx.connection).selectLong("select id from transactionspec where id > 1000")
            }
        }
      }
    }

    describe("executeBatch") {
      it("should return the number of inserted records") {
        case class Item(v1: Long, v2: String)
        val items = Seq(Item(1, "test"), Item(1, "test"))
        val count = database.transaction {
          tx =>
            tx.executeBatch("insert into transactionspec values(?, ?)") {
              statement =>
                var counter = 0
                items.foreach {
                  item =>
                    counter += (statement << item.v1 << item.v2 <<!)
                }
                counter
            }
        }

        count should equal(items.size)
      }

      it("should return the number of updated records") {
        val itemsToUpdate = Seq(23, 42, 38, 232)
        val existingItems = Seq(23, 42)
        val count = database.transaction {
          tx =>
            tx.executeBatch("update transactionspec set name='foo' where id=?") {
              statement =>
                var counter = 0
                itemsToUpdate.foreach {
                  item =>
                    counter += (statement << item <<!)
                }
                counter
            }
        }

        count should equal(existingItems.size)
      }

      it("should be faster than normal execute-calls") {
        case class Item(v1: Long, v2: String)

        def executeBatch(query: String, items: Iterable[Item], tx: Transaction): Long = {
          val start = System.currentTimeMillis
          tx.executeBatch(query) {
            statement =>
              items.foreach {
                item =>
                  statement << item.v1 << item.v2 <<!
              }
          }
          System.currentTimeMillis - start
        }

        def normalExecute(query: String, items: Iterable[Item], tx: Transaction): Long = {
          val start = System.currentTimeMillis
          items.foreach {
            item =>
              tx.execute(query, item.v1, item.v2)
          }
          System.currentTimeMillis - start
        }


        val size = 1000
        val items = {
          val tmp = new scala.collection.mutable.ArrayBuffer[Item]
          for (i <- 0 until size) {
            tmp += Item(i, "foo" + i)
          }
          tmp
        }

        database.transaction {
          tx =>
            val sql = "insert into transactionspec values(?, ?)"
            val normalTiming = normalExecute(sql, items, tx)
            val batchTiming = executeBatch(sql, items, tx)
            val difference: Double = normalTiming / batchTiming
            println(s"normal time: $normalTiming")
            println(s"batch time: $batchTiming")
            println("executing " + size + " made a difference of " + (difference * 100) + "%")
            difference should be > (1.0)
        }
      }

      it( "should handle nullable columns correctly" ) {
        case class Item( v1: Long, v2: Option[String] )

        val items = List( Item( 1001, Some( "foo" ) ), Item( 1002, None ) )
        val strNullable = (x:Option[String]) => Nullable(x.map(StringFormattable(_)))

        database.transaction { tx =>
          tx.executeBatch( "insert into transactionspec values(?, ?)" ) { statement =>
            items.foreach { item =>
              statement << item.v1 << Nullable(item.v2.map(StringFormattable(_))) <<!
            }
          }
        }

        database.transaction { tx =>
          val rs = Database(tx.connection).select( "select id, name from transactionspec where id > 1000" ) { row =>
            Item( row, row )
          }

          rs should contain (items.head)
          rs should contain (items.last)
        }
      }
    }
    describe("Procedures and functions"){
      it ("should call properly the stored procedure and return 12345"){
        database.transaction{
          tx =>
            val l = tx.callProcedure("call new_transactionspec(?, ?, ?)", ParameterIn(Some(10000), Types.INTEGER), ParameterIn(Some("pippo"), Types.VARCHAR),ParameterOut(Types.INTEGER))
            l should equal (List(12345))
        }
      }

      it ("should call properly the function and return the correct time"){
        database.transaction{
          tx =>
            val timestamp = new java.sql.Timestamp(new java.util.Date().getTime)
            val l = tx.callFunction("{call an_hour_before(?)}", ParameterIn(Some(timestamp), Types.TIMESTAMP))
            println(l)
        }
      }

    }
  }
}