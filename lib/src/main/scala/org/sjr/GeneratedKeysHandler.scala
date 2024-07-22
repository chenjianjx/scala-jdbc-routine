package org.sjr

/**
 *
 * @tparam KEYS Can be a single key or a collection, depending on if single or batch update is executed, and also on the DB vendor's JDBC implementation
 */
trait GeneratedKeysHandler[KEYS] {
  def handle(resultSet: WrappedResultSet): Option[KEYS]
}