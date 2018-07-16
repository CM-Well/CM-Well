package cmwell.analytics.util

object StringUtil {

  /**
    * Split a string into lines.
    * Depending on the source, the delimiter can be \r or \n.
    * If the string contains \r, assume the delimiter is \r, otherwise it is \n
    */
  def splitLines(x: String): Array[String] = {
    // Does the file use \r or \n to delimit lines?
    val delimiter = if (x.contains('\r')) "\r" else "\n"

    x.split(delimiter)
      .map(_.trim) // trim pesky whitespace that can be left behind (e.g., by \r\n)
  }

}
