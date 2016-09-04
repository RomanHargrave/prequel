package net.noerd.prequel

import java.sql.{CallableStatement, Connection, SQLException, Types}
import java.util.IllegalFormatException

import net.noerd.prequel.RichConnection.conn2RichConn
import org.slf4j.LoggerFactory

/**
  * A Transaction is normally created by the InTransaction object and can be
  * used to execute one or more queries against the database. Once the block
  * passed to InTransaction is succesfully executed the transaction is auto-
  * matically committed. And if some exception is throws during execution the
  * transaction is rollbacked.
  *
  * @throws SQLException           all methods executing queries will throw SQLException
  *                                if the query was not properly formatted or something went wrong in
  *                                the database during execution.
  * @throws IllegalFormatException : Will be throw by all method if the format
  *                                string is invalid or if there is not enough parameters.
  */
class Transaction(val connection: Connection, val formatter: SQLFormatter) {
    private val logger = LoggerFactory.getLogger("Transaction")

  /**
    * Executes the given query and returns the number of affected records
    *
    * @param sql    query that must not return any records
    * @param params are the optional parameters used in the query
    * @return the number of affected records
    */
  def execute(sql: String, params: Formattable*): Int = {
    /*
    connection.usingStatement { statement =>
        statement.executeUpdate( formatter.formatSeq( sql, params.toSeq ) )
    }
    */
    val (sql2, params2) = formatter.formatSeq(sql, params.toSeq)
    connection.usingReusableStatement(sql2, formatter, false) {
      statement =>
        statement.executeWith(params2.toSeq: _*)
    }
  }

  /**
    * Will pass a ReusableStatement to the given block. This block
    * may add parameters to the statement and execute it multiple times.
    * The statement will be automatically closed onced the block returns.
    *
    * Example:
    * tx.executeBatch( "insert into foo values(?)" ) { statement =>
    * items.foreach { statement.executeWith( _ ) }
    * }
    *
    * @return the result of the block
    * @throws SQLException if the query is missing parameters when executed
    *                      or if they are of the wrong type.
    */
  def executeBatch[T](sql: String, generateKeys: Boolean = false)(block: (ReusableStatement) => T): T = {
    connection.usingReusableStatement(sql, formatter, generateKeys)(block)
  }

  /**
    * Excecutes a call to a stored procedure.
    * Accepts ProcedureParameter array for in and out.
    *
    * @param procedureCall the db statement to call the  procedure
    * @param procedureParameters all the IN, OUT or INOUT parameters
    * @return Seq[Any] to match all cases
    */
  def callProcedure(procedureCall: String, procedureParameters: ProcedureParameter*): Seq[Any] = {
    connection.usingCallableStatement(procedureCall) {
      st =>
        loadParameters(st, procedureParameters)
        // execute function or procedure
        st.execute()
        // search for results...

        // if parameters are all of type ParamaterIn there are no results!
        if (procedureParameters.forall(param => param.getClass.getSimpleName == ParameterIn.getClass.getSimpleName)) {
          logger.debug("No Parameter IN or INOUT defined")
          Seq.empty[Any]
        } else {
          // there is almost one ParameterOut or ParameterInOut...
          val results: Seq[Any] = searchForResult(st, procedureParameters)
          results.filter(_ != (()))
        }

    }
  }

  /**
    * Excecutes a call to a function .
    * Accepts ProcedureParameter array for in and out.
    *
    * @param functionCall the db statement to call the function
    * @param functionParameters all the IN, OUT or INOUT parameters
    * @return Seq[Any] to match all cases
    */
  def callFunction(functionCall: String, functionParameters: ProcedureParameter*): Seq[Any] = {
    connection.usingCallableStatement(functionCall) {
      st =>
        // side effect!!!! remove it later!!!!
        loadParameters(st, functionParameters)
        // execute function or procedure
        st.execute()
        // search for results...
        // if parameters are all of type ParamaterIn there are no results!
        // except for HSQLDB which does not have an out parameter in function calls...
        if (st.getConnection.getMetaData.getDatabaseProductName.toUpperCase.contains("HSQL")) {
          val rs = st.getResultSet
          rs.next()
          val result = rs.getObject(1)
          Seq(result)
        } else if (functionParameters.forall(param => param.getClass.getSimpleName == ParameterIn.getClass.getSimpleName)) {
          logger.debug("No Parameter IN or INOUT defined")
          Seq.empty[Any]
        } else {
          // there is almost one ParameterOut or ParameterInOut...
          val results: Seq[Any] = searchForResult(st, functionParameters)
          results.filter(_ != (()))
        }

    }
  }

  /**
    * Load parameters properly on this CallableStatement.
    * @param st the CallableStatement
    * @param p the parameters sequence
    */
  private[Transaction] def loadParameters(st: CallableStatement, p: Seq[ProcedureParameter]): Unit = {

    var index = 0

    for (parameter <- p) {
      index = index + 1
      parameter match {
        // Handle any IN (or INOUT) types: If the optional value is None, set it to NULL, otherwise, map it according to
        // the actual object value and type encoding:
        case p: ParameterOut => st.registerOutParameter(index, p.parameterType)
        case ParameterIn(None, t) => st.setNull(index, t)
        case ParameterIn(v: Some[_], Types.DATE) => st.setDate(index, v.get.asInstanceOf[java.sql.Date])
        case ParameterIn(v: Some[_], Types.TIMESTAMP) => st.setTimestamp(index, v.get.asInstanceOf[java.sql.Timestamp])
        case ParameterIn(v: Some[_], Types.NUMERIC | Types.DECIMAL) => st.setBigDecimal(index, v.get.asInstanceOf[java.math.BigDecimal])
        case ParameterIn(v: Some[_], Types.BIGINT) => st.setLong(index, v.get.asInstanceOf[Long])
        case ParameterIn(v: Some[_], Types.INTEGER) => st.setInt(index, v.get.asInstanceOf[Int])
        case ParameterIn(v: Some[_], Types.VARCHAR | Types.LONGVARCHAR) => st.setString(index, v.get.asInstanceOf[String])
        case ParameterIn(v: Some[_], Types.CHAR) => st.setString(index, v.get.asInstanceOf[String].head.toString)
        case ParameterInOut(None, t) => st.setNull(index, t)

        // Now handle all of the OUT (or INOUT) parameters, these we just need to set the return value type:
        case ParameterInOut(v: Some[_], Types.DATE) =>
          st.setDate(index, v.get.asInstanceOf[java.sql.Date])
          st.registerOutParameter(index, Types.DATE)
        case ParameterInOut(v: Some[_], Types.TIMESTAMP) =>
          st.setTimestamp(index, v.get.asInstanceOf[java.sql.Timestamp])
          st.registerOutParameter(index, Types.TIMESTAMP)
        case ParameterInOut(v: Some[_], Types.NUMERIC) =>
          st.setBigDecimal(index, v.get.asInstanceOf[java.math.BigDecimal])
          st.registerOutParameter(index, Types.NUMERIC)
        case ParameterInOut(v: Some[_], Types.DECIMAL) =>
          st.setBigDecimal(index, v.get.asInstanceOf[java.math.BigDecimal])
          st.registerOutParameter(index, Types.DECIMAL)
        case ParameterInOut(v: Some[_], Types.BIGINT) =>
          st.setLong(index, v.get.asInstanceOf[Long])
          st.registerOutParameter(index, Types.BIGINT)
        case ParameterInOut(v: Some[_], Types.INTEGER) =>
          st.setInt(index, v.get.asInstanceOf[Int])
          st.registerOutParameter(index, Types.INTEGER)
        case ParameterInOut(v: Some[_], Types.VARCHAR) =>
          st.setString(index, v.get.asInstanceOf[String])
          st.registerOutParameter(index, Types.VARCHAR)
        case ParameterInOut(v: Some[_], Types.LONGVARCHAR) =>
          st.setString(index, v.get.asInstanceOf[String])
          st.registerOutParameter(index, Types.LONGVARCHAR)
        case ParameterInOut(v: Some[_], Types.CHAR) =>
          st.setString(index, v.get.asInstanceOf[String].head.toString)
          st.registerOutParameter(index, Types.CHAR)
        case _ =>
          logger.debug(s"Failed to match ProcedureParameter in executeFunction (IN): index $index (${parameter.toString})")
      }
    }
  }

  /**
    * Scans the IN o INOUT parameters searching for results.
    * @param st the CallableStatement
    * @param p the parameters sequence
    * @return a sequence of [Any] which containd the - eventually produced - results
    */
  private [Transaction] def searchForResult(st: CallableStatement, p: Seq[ProcedureParameter]): Seq[Any] = {
    var index = 0
    for (parameter <- p) yield {
      index = index + 1
      parameter match {
        case ParameterOut(Types.DATE) => st.getDate(index)
        case ParameterOut(Types.TIMESTAMP) => st.getTimestamp(index)
        case ParameterOut(Types.NUMERIC) | ParameterOut(Types.DECIMAL) => st.getBigDecimal(index)
        case ParameterOut(Types.BIGINT) => st.getLong(index)
        case ParameterOut(Types.INTEGER) => st.getInt(index)
        case ParameterOut(Types.VARCHAR | Types.LONGVARCHAR | Types.CHAR) => st.getString(index)
        case ParameterInOut(v: Some[_], Types.DATE) => st.getDate(index)
        case ParameterInOut(v: Some[_], Types.TIMESTAMP) => st.getTimestamp(index)
        case ParameterInOut(v: Some[_], Types.NUMERIC | Types.DECIMAL) => st.getInt(index)
        case ParameterInOut(v: Some[_], Types.BIGINT) => st.getLong(index)
        case ParameterInOut(v: Some[_], Types.INTEGER) => st.getInt(index)
        case ParameterInOut(v: Some[_], Types.VARCHAR | Types.LONGVARCHAR | Types.CHAR) => st.getString(index)
        case _ => {
          logger.debug(s"Failed to match ProcedureParameter in executeFunction (OUT): index $index (${parameter.toString})")
        }
      }
    }
  }

  /**
    * Rollbacks the Transaction.
    *
    * @throws SQLException if transaction could not be rollbacked
    */
  def rollback(): Unit = connection.rollback()

  /**
    * Commits all changed done in the Transaction.
    *
    * @throws SQLException if transaction could not be committed.
    */
  def commit(): Unit = connection.commit()

  /**
    * Verify if connection is still alive
    */
  def active_? = !connection.isClosed

}

object Transaction {

  def apply(conn: Connection, formatter: SQLFormatter) = new Transaction(conn, formatter)
}