package cmwell.tools.neptune.export

import java.io.{BufferedWriter, File, FileWriter}

import cmwell.tools.neptune.export.EikonMain.bw
import net.liftweb.json.DefaultFormats
import net.liftweb.json._

import scala.io.Source

case class SearchMetaData(
                         key: String,
                         visibility: String,
                         value: String
                       )
case class EikonNtriple(eventId:String, eventTimestamp:String, metadata:List[Map[String, Any]])
//TODO:add timestamp - very important

object EikonMain extends App{


  implicit val formats = DefaultFormats
  val file = new File("/home/liel/Desktop/Eikon/search-event.nq")
  val bw = new BufferedWriter(new FileWriter(file, true))

  Source.fromFile("/home/liel/Desktop/Eikon/2019-01-19.json").getLines.map(line=> parse(line)).
    map(json => EikonNtriple((json \ "event-id").extract[String], (json \ "event-timestamp").extract[String], extractAllMedata(json))).
    map(eikonTriple => (eikonTriple, eikonTriple.metadata.map(metaData=> "<http://graph.link/ees/E-" + eikonTriple.eventId + "> <http://graph.link/ees/" + metaData("key") + "> " + manipulateValue(metaData) + " .")))
    .foreach(ntriple => {
      ntriple._2.foreach(line=>  {
        bw.write(line  + "\n")
      }
    )
      bw.write("<http://graph.link/ees/E-" + ntriple._1.eventId + "> " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" + " <http://www.w3.org/ns/oa#Event> .\n")
      bw.write("<http://graph.link/ees/E-" + ntriple._1.eventId + "> " + "<http://graph.link/ees/eventTimeStamp> " +  "\"" + ntriple._1.eventTimestamp + "\"" +"^^<http://www.w3.org/2001/XMLSchema#double>"  + " .\n")
    })

  bw.close()

  private def manipulateValue(metaData: Map[String, Any]) = {
     if(scala.util.Try(metaData("value").toString.toBoolean).isSuccess)
      "\"" + metaData("value").toString.toBoolean + "\"" + "^^<http://www.w3.org/2001/XMLSchema#boolean>"
    else {
      if (metaData("key") == "request-country" || metaData("key") == "selected-entity-symbol")
        "\"" + metaData("value") + "\""
      else {
        val x = metaData("value").toString.replace("""\""", "").replace("\"", "")
        if (scala.util.Try(x.toInt).isSuccess)
          "\"" + x + "\"" + "^^<http://www.w3.org/2001/XMLSchema#integer>"
        else if (scala.util.Try(x.toDouble).isSuccess)
          "\"" + x + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>"
        else if (scala.util.Try(x.toLong).isSuccess)
          "\"" + x + "\"" + "^^<http://www.w3.org/2001/XMLSchema#long>"
        else
          "\"" + x + "\""
      }
    }

  }

  def extractAllMedata(jsonObj:JValue) = {
    val tokens:List[Map[String, Any]] = for {
      // tokenList are:
      // JArray(List(JObject(List(JField(word,JString(Some)), JField(lemma,JString(Some)))), JObject(List(JField(word,JString(text)), JField(lemma,JString(text))))))
      // JArray(List(JObject(List(JField(word,JString(and)), JField(lemma,JString(And)))), JObject(List(JField(word,JString(here)), JField(lemma,JString(here))))))
      tokenList <- (jsonObj \\ "metadata").children
      // subTokenList are:
      // List(JObject(List(JField(word,JString(Some)), JField(lemma,JString(Some)))), JObject(List(JField(word,JString(text)), JField(lemma,JString(text)))))
      // List(JObject(List(JField(word,JString(and)), JField(lemma,JString(And)))), JObject(List(JField(word,JString(here)), JField(lemma,JString(here)))))
      JArray(subTokenList) <- tokenList
      // liftToken are:
      // JObject(List(JField(word,JString(Some)), JField(lemma,JString(Some))))
      // JObject(List(JField(word,JString(text)), JField(lemma,JString(text))))
      // JObject(List(JField(word,JString(and)), JField(lemma,JString(And))))
      // JObject(List(JField(word,JString(here)), JField(lemma,JString(here))))
      liftToken <- subTokenList
      // token are:
      // Token(Some,Some)
      // Token(text,text)
      // Token(and,And)
      // Token(here,here)
      token = liftToken.extract[JObject].values

    } yield token
    tokens
  }

//  for (acct <- elements) {
//    val m = acct.extract[SearchMetaData]
//    println(s"Account: ${m.url}, ${m.username}, ${m.password}")
//  }


}
