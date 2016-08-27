package net.noerd.prequel

/**
 * The base for any parameter used in a sql query.
 *
 * @see Formattables.scala for examples on implementations.
 * @see SQLFormatter for escaping and quoting of strings.
 */
trait Formattable {
  /**
   * Must return a sql escaped string of the parameter
   */
  def escaped(formatter: SQLFormatter): String

  /**
   * Used when doing batch inserts or updates. Should use
   * the given ReusableStatement to add the parameter.
   */
  def addTo(statement: ReusableStatement): Unit

  /**
   * Should return the parameter as it is
   */
  def value: Any
}