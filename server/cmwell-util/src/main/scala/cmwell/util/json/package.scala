/**
  * © 2019 Refinitiv. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package cmwell.util

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 7/23/13
  * Time: 11:27 AM
  * To change this template use File | Settings | File Templates.
  */
package object json {

  def escapeString(s: String) = s.split('\\').mkString("\\\\").split('"').mkString("\\\"")

  /**
    * convert a map of: {"infoton path" -> map_of{"attribute" -> set[values]}} to suitable bulk json form
    * @param infotonsMap
    * @return
    */
  def infotonsMapToBulkJson(infotonsMap: Map[String, Map[String, Set[String]]]): String = {
    /*
     * convert a single infoton map of: path -> map of attributes to values into json form
     * @param infotonsMap
     * @return
     */
    def infotonsTemplate(infotonsMap: Map[String, Map[String, Set[String]]]) = {
      /*
       * convert a map of attribute -> values to json form
       */
      def fieldsTemplate(attrMap: Map[String, Set[String]]): String =
        attrMap
          .map {
            case (k, v) => "\"%s\":[%s]".format(escapeString(k), v.map(escapeString(_)).mkString("\"", "\",\"", "\""))
          }
          .mkString(",")

      //generate array of infotons
      (for {
        path <- infotonsMap.keySet
        infoton = """
                    |{
                    |    "type": "ObjectInfoton",
                    |    "system": {
                    |        "path": "%s"
                    |    },
                    |     "fields": {
                    |        %s
                    |    }
                    |}
                  """.stripMargin.filterNot(_.isWhitespace).format(path, fieldsTemplate(infotonsMap(path)))
      } yield infoton).mkString(",")
    }

    //generate bulk
    """
        |{
        |    "type":"BagOfInfotons",
        |    "infotons":[
        |        %s
        |    ]
        |}
    """.stripMargin.filterNot(_.isWhitespace).format(infotonsTemplate(infotonsMap))
  }

}
