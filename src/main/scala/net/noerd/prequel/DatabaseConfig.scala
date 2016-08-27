package net.noerd.prequel

import java.sql.Connection.TRANSACTION_NONE
import java.sql.Connection.TRANSACTION_READ_COMMITTED
import java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
import java.sql.Connection.TRANSACTION_REPEATABLE_READ
import java.sql.Connection.TRANSACTION_SERIALIZABLE
import java.sql.SQLException

/**
  * Trait to implement java enum on isolation levels
  * @param id the isolation level
  */
sealed abstract class TransactionIsolation(val id: Int)

/**
  * Isolation levels
  */
object IsolationLevels {

  case object None extends TransactionIsolation(TRANSACTION_NONE)

  case object ReadCommitted extends TransactionIsolation(TRANSACTION_READ_COMMITTED)

  case object ReadUncommitted extends TransactionIsolation(TRANSACTION_READ_UNCOMMITTED)

  case object RepeatableRead extends TransactionIsolation(TRANSACTION_REPEATABLE_READ)

  case object Serializable extends TransactionIsolation(TRANSACTION_SERIALIZABLE)

}

/**
 * Configures how to connect to the database and how the connection
 * should then be pooled.
 *
 * @param driver the driver classname
 * @param jdbcURL the database url string
 * @param username username to acces the database
 * @param password user password
 * @param isolationLevel sets the connection isolation level
 * @param sqlFormatter the formatter object used to format sql strings
 * @param dataSourceClassName the class name of the datasource
 * @param serverName the db server name
 * @param serverPort the db server port
 * @param databaseName the database name
 */
final case class DatabaseConfig(
                                 driver: String = "",
                                 jdbcURL: String = "",
                                 username: String = "",
                                 password: String = "",
                                 isolationLevel: TransactionIsolation = IsolationLevels.ReadCommitted,
                                 sqlFormatter: SQLFormatter = SQLFormatter.DefaultSQLFormatter,
                                 dataSourceClassName: String = "",
                                 serverName: String = "",
                                 serverPort: String = "",
                                 databaseName: String = "",
                                 autoCommit: Boolean = false,
                                 maximumPoolSize: Int = 8
                                 ) {

  // Make sure that the class is available
  if (driver.length > 0) Class.forName(driver)

  /**
   * Execute the block in a transaction against the db defined by
   * the configuration.
   *
   * If the block is executed succesfully the transaction
   * will be committed but if an exception is throw it will be rolled back
   * immediately and rethrow the exception.
   *
   * @throws Any Exception that the block may generate.
   * @throws SQLException if the connection could not be committed, rollbacked
   *             or closed.
   */
  def transaction[T](block: (Transaction) => T) = InTransaction(block, this)
}

