package net.noerd.prequel

import java.sql.Connection.TRANSACTION_NONE
import java.sql.Connection.TRANSACTION_READ_COMMITTED
import java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
import java.sql.Connection.TRANSACTION_REPEATABLE_READ
import java.sql.Connection.TRANSACTION_SERIALIZABLE
import java.sql.SQLException


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
 * @param properties connection properties
 */
final case class DatabaseConfig(
                                       driver: String = "",
                                       dataSourceClassName: String = "",
                                       jdbcURL: String = "",
                                       maximumPoolSize: Int = 8,
                                       autoCommit: Boolean = false,
                                       sqlFormatter: SQLFormatter = SQLFormatter.DefaultSQLFormatter,
                                       properties: Map[String, AnyRef] = Map.empty
                               ) {

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
