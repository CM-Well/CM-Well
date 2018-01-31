package cmwell.tools.data.sparql

import cmwell.tools.data.downloader.consumer.Downloader.Token

import scala.concurrent.ExecutionContext

object StpUtil {
  def headerString(header: (String, String)): String = header._1 + ":" + header._2

  def headersString(headers: Seq[(String, String)]): String = headers.map(headerString).mkString("[", ",", "]")


  def extractLastPart(path: String) = {
    val p = if (path.endsWith("/")) path.init
    else path
    val (_, name) = p.splitAt(p.lastIndexOf("/"))
    name.tail.init
  }

  def readPreviousTokens(baseUrl: String, path: String, format: String)(implicit context : ExecutionContext) = {
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

    cmwell.util.http.SimpleHttpClient.get(s"http://$baseUrl$path/tokens?op=stream&recursive&format=$format")
      .map(
        _.payload.split("\n")
          .map(_.split(" "))
          .collect { case Array(s, p, o, _) =>
            val token = if (o.startsWith("\"")) o.init.tail else o
            extractLastPart(s) -> token
          }
          .foldLeft(Map.empty[String, Token])(_ + _)
      )
  }


}
