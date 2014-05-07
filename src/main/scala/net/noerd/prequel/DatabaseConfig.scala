package net.noerd.prequel

import java.sql.Connection.TRANSACTION_NONE
import java.sql.Connection.TRANSACTION_READ_COMMITTED
import java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
import java.sql.Connection.TRANSACTION_REPEATABLE_READ
import java.sql.Connection.TRANSACTION_SERIALIZABLE


sealed abstract class TransactionIsolation(val id: Int)

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
 * @param driver
 * @param jdbcURL
 * @param username
 * @param password
 * @param isolationLevel
 * @param sqlFormatter
 * @param dataSourceClassName
 * @param serverName
 * @param serverPort
 * @param databaseName
 */
final case class DatabaseConfig(
                                 val driver: String = "",
                                 val jdbcURL: String = "",
                                 val username: String = "",
                                 val password: String = "",
                                 val isolationLevel: TransactionIsolation = IsolationLevels.ReadCommitted,
                                 val sqlFormatter: SQLFormatter = SQLFormatter.DefaultSQLFormatter,
                                 val dataSourceClassName: String = "",
                                 val serverName: String = "",
                                 val serverPort: String = "",
                                 val databaseName: String = "",
                                 val autoCommit: Boolean = false,
                                 val maximumPoolSize: Int = 10
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
   *                      or closed.
   */
  def transaction[T](block: (Transaction) => T) = InTransaction(block, this)
}

