package ld.exceptions

/**
  * Proj: server
  * User: gilad
  * Date: 2/13/18
  * Time: 11:44 AM
  */
sealed trait ServerComponentNotAvailableException extends Exception
object ServerComponentNotAvailableException {
  def apply(msg: String) =
    new Exception(msg) with ServerComponentNotAvailableException
  def apply(msg: String, cause: Throwable) =
    new Exception(msg, cause) with ServerComponentNotAvailableException
}
