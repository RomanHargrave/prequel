package net.noerd.prequel


import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach

class ConnectionPoolsSpec extends FunSpec with Matchers with BeforeAndAfterEach {

  val config1 = DatabaseConfig(
    driver = "org.hsqldb.jdbc.JDBCDriver",
    jdbcURL = "jdbc:hsqldb:mem:config1",
    sqlFormatter = SQLFormatter.HSQLDBSQLFormatter,
    autoCommit = false,
    maximumPoolSize = 1
  )
  val config1Copy = DatabaseConfig(
    driver = "org.hsqldb.jdbc.JDBCDriver",
    jdbcURL = "jdbc:hsqldb:mem:config1",
    sqlFormatter = SQLFormatter.HSQLDBSQLFormatter,
    autoCommit = false,
    maximumPoolSize = 1
  )
  val config2 = DatabaseConfig(
    driver = "org.hsqldb.jdbc.JDBCDriver",
    jdbcURL = "jdbc:hsqldb:mem:config2",
    sqlFormatter = SQLFormatter.HSQLDBSQLFormatter,
    autoCommit = false,
    maximumPoolSize = 1
  )


  override def beforeEach() = {
    ConnectionPools.reset()
  }

  describe("ConnectionPools") {

    describe("getOrCreatePool") {
       // commented cause TRAVIS-CI can't run properly
      /*it("should create a new pool for each unique Configuration") {
        ConnectionPools.reset

        ConnectionPools.getOrCreatePool(config1)
        ConnectionPools.nbrOfPools should be(1)

        ConnectionPools.getOrCreatePool(config2)
        ConnectionPools.nbrOfPools should be(2)
      }

      it("should reuse an existing pool if the configuration is the same") {

        ConnectionPools.getOrCreatePool(config1)
        ConnectionPools.nbrOfPools should be(1)

        ConnectionPools.getOrCreatePool(config1)
        ConnectionPools.nbrOfPools should be(1)

        ConnectionPools.getOrCreatePool(config1Copy)
        ConnectionPools.nbrOfPools should be(1)
      }*/
    }
  }

}