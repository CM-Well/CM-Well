package ld.exceptions

/**
  * Proj: server
  * User: gilad
  * Date: 2/13/18
  * Time: 11:44 AM
  */
sealed trait ConflictingNsEntriesException extends Exception
object ConflictingNsEntriesException {
  def format(kind: String, entry: String, nsIDs: Iterable[String]): String = nsIDs.mkString(s"Conflicting ns identifiers found for $kind [$entry] [",",","]")

  def byURL(url: String, nsIDs: Iterable[String]) = new Exception(format("url",url,nsIDs)) with ConflictingNsEntriesException
  def byURL(url: String, nsIDs: Iterable[String], cause: Throwable) = new Exception(format("url",url,nsIDs), cause) with ConflictingNsEntriesException
  def byPrefix(prefix: String, nsIDs: Iterable[String]) = new Exception(format("prefix",prefix,nsIDs)) with ConflictingNsEntriesException
  def byPrefix(prefix: String, nsIDs: Iterable[String], cause: Throwable) = new Exception(format("prefix",prefix,nsIDs), cause) with ConflictingNsEntriesException
}