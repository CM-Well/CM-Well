/**
  * Copyright 2015 Thomson Reuters
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


package cmwell.ws

import cmwell.domain._
import cmwell.fts.{ Settings => _, _ }
import cmwell.util.collections._
import cmwell.util.exceptions.trySequence
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.{ PrefixAmbiguityException, UnretrievableIdentifierException }
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import org.joda.time.format.{ DateTimeFormat, ISODateTimeFormat }
import org.joda.time.{ DateTime, DateTimeZone }

import scala.concurrent.{ Await, Future, Promise }
import scala.util.parsing.combinator.{ JavaTokenParsers, RegexParsers }
import scala.util.{ Failure => UFailure, Success => USuccess, Try }
import wsutil._

package object util {

  type Uuids = Set[String]

  case class SortedIteratorState(currentTimeStamp: Long, fieldFilters: Option[FieldFilter], from: Option[DateTime], to: Option[DateTime], path: Option[String], withDescendants: Boolean, withHistory: Boolean)
}

package util {

  import cmwell.web.ld.exceptions.ParsingException

  import scala.annotation.tailrec

  trait PrefixRequirement {
    @inline final protected[this] def prefixRequirement(requirement: Boolean, message: => String): Unit = {
      if (!requirement)
        throw new UnretrievableIdentifierException(message)
    }
  }

  class RegexAndFieldNameParser extends JavaTokenParsers with LazyLogging with PrefixRequirement {

    protected[this] val fieldRegex = """[\w+\-*.$]+""".r

    val namespaceUri: Parser[String] = """^(([^:/?#$]+):)?(//([^$/?#]*))?([^$?#]*)(\?([^$#]*))?(#([^$]*))?""".r

    val cmwellUriPrefix: Parser[String] = {

      /* ******************* *
       *  REGEX explanation: *
       * ******************* *

  (                                                                          1.  start a group to capture protocol + optional domain
   (cmwell:/)                                                                2.  if protocol is `cmwell:` no need to specify domain
             |                                                               3.  OR
              (https?://                                                     4.  if protocol is `http:` or `https:` (we need a domain)
                        (                                                    5.  start a group to capture multiple domain parts that end with a '.'
                         (?!-)                                               6.  domain part cannot begin with hyphen '-'
                              [A-Za-z0-9-]{1,63}                             7.  domain part is composed of letters, digits & hyphen and can be in size of at least 1 but less than 64
                                                (?<!-)                       8.  domain part cannot end with an hyphen (look back)
                                                      \.)*                   9.  all parts except the last must end with a dot '.'
                        (                                                    10. start same regex as before for the last domain part which must not end with a dot '.'
                         (?!-)                                               11. same as 6
                              [A-Za-z0-9-]{1,63}                             12. same as 7
                                                (?<!-)                       13. same as 8
                                                      )                      14. end domain group
                                                       (:\d+)?               15. optional port
                                                              )              16. end http/https with domain and optional port group
                                                               )             17. end cmwell OR http group
                                                                /meta/       18. path must be prefixed with `/meta/`
  */
      """((cmwell:/)|(http://((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)*((?!-)[A-Za-z0-9-]{1,63}(?<!-))(:\d+)?))/meta/""".r
    }

    val cmwellUriType: Parser[String] = literal("sys") | literal("nn")

    val ncName: Parser[String] = "[^$]+".r ^? {
      case name if org.apache.xerces.util.XMLChar.isValidNCName(name) => name
    }

    val cmwellUri: Parser[String] = cmwellUriPrefix ~> cmwellUriType ~ "#" ~ ncName ^? {
      case "sys" ~ _ ~ localName => s"system.$localName"
      case "nn" ~ _ ~ localName => localName
    }

    val uri: Parser[Either[UnresolvedFieldKey,DirectFieldKey]] = cmwellUri ^^ { s => Right(NnFieldKey(s))
    } | namespaceUri ^^ { u =>
      Left(UnresolvedURIFieldKey(u))
    }

    val fullPredicateURI: Parser[Either[UnresolvedFieldKey,DirectFieldKey]] = "$" ~> uri <~ "$"

    val fieldParser: Parser[Either[UnresolvedFieldKey,DirectFieldKey]] = fullPredicateURI | (fieldRegex ^^ { s =>
      s.lastIndexOf('.') match {
        case -1 => Right(NnFieldKey(s))
        case i => s.splitAt(i) match {
          case (first, _) if first == "system" || first.startsWith("system.") => Right(NnFieldKey(s))
          case (first, dotLast) => dotLast.tail match {
            case t if t.isEmpty => {
              logger.warn(s"field ending with '.' : $s")
              Right(NnFieldKey(s))
            }
            case t if t.head == '$' => Right(HashedFieldKey(first, t.tail))
            case t => Left(UnresolvedPrefixFieldKey(first, t))
          }
        }
      }
    })

    val fieldsParser = repsep(fieldParser, ",")

  }

  object FieldKeyParser extends RegexAndFieldNameParser {
    def fieldKey(fieldRepr: String) = parseAll(fieldParser, fieldRepr) match {
      case Success(result, _) => USuccess(result)
      case NoSuccess(msg, _) => UFailure(new IllegalArgumentException(msg))
    }
  }

  object FieldNameConverter extends RegexAndFieldNameParser {
    def convertAPINotationToActualFieldNames(displayNames: String): Try[Seq[Either[UnresolvedFieldKey,DirectFieldKey]]] = {
      parseAll(fieldsParser, displayNames) match {
        case Success(result, _) => USuccess(result)
        case NoSuccess(msg, _) => UFailure(new IllegalArgumentException(msg))
      }
    }

    def toActualFieldNames(s: String) = convertAPINotationToActualFieldNames(s)
  }

  object AggregationsFiltersParser extends RegexAndFieldNameParser {

    lazy val sLiteral = """(?:[\p{L}\p{Sc}0-9/.@ &_\-]+)""".r
    lazy val positiveInteger = """\d+""".r
    lazy val pNumber = """\d+""".r

    def aggregationsParser: Parser[List[RawAggregationFilter]] = repsep(aggregationParser, "~")

    def aggregationParser: Parser[RawAggregationFilter] =
      termAggregationParser | statsAggregationParser | histAggregationParser | sigTermsAggregationParser | cardAggregationParser

    def subAggregationsParser: Parser[Seq[RawAggregationFilter]] = ("<" ~ aggregationsParser ~ ">").? ^^ {
      case Some(_ ~ subAggs ~ _) => subAggs
      case None => Seq.empty
    }


    def nameParser: Parser[String] = ",name:" ~ sLiteral ^^ {
      case _ ~ n => n
    }

    //TODO: use `RegexAndFieldNameParser.fieldParser` for real field name
    def rawFieldParser: Parser[RawField[FieldValeOperator]] = ",field" ~> ("::" | ":") ~ fieldParser ^^ {
      case "::" ~ f => NonAnalyzedField -> f
      case ":" ~ f => AnalyzedField -> f
    }

    def sizeParser: Parser[Int] = ",size:" ~ pNumber ^^ {
      case _ ~ s => Integer.parseInt(s)
    }

    def intervalParser: Parser[Int] = ",interval:" ~ pNumber ^^ {
      case _ ~ i => Integer.parseInt(i)
    }

    def minDocCountParser: Parser[Int] = ",minDocCount:" ~ pNumber ^^ {
      case _ ~ mdc => Integer.parseInt(mdc)
    }

    def extMinParser: Parser[Long] = ",extMin:" ~ pNumber ^^ {
      case _ ~ em => em.toLong
    }

    def extMaxParser: Parser[Long] = ",extMax:" ~ pNumber ^^ {
      case _ ~ em => em.toLong
    }

    def backgroundTermParser: Parser[(String, String)] = ",backgroundTerm:" ~ sLiteral ~ "*" ~ sLiteral ^^ {
      case _ ~ key ~ _ ~ value => (key, value)
    }

    def precisionThresholdParser: Parser[Long] = ",precisionThreshold:" ~ pNumber ^^ {
      case _ ~ pt => pt.toLong
    }

    def dTermAggFilter = log(termAggregationParser)("term_agg_filter")

    def termAggregationParser: Parser[RawTermAggregationFilter] = "type:term" ~ nameParser.? ~ rawFieldParser ~ sizeParser.? ~
      subAggregationsParser ^^ {
        case _ ~ nameOpt ~ field ~ sizeOpt ~ subAggs =>
          val name = nameOpt.getOrElse("TermAggregation")
          val size = sizeOpt.getOrElse(10)
          RawTermAggregationFilter(name, field, size, subAggs)
      }

    def dStatsAggFilter = log(statsAggregationParser)("stats_parser")

    def statsAggregationParser: Parser[RawStatsAggregationFilter] = "type:stats" ~ nameParser.? ~ rawFieldParser ^^ {
      case _ ~ nameOpt ~ field =>
        val name = nameOpt.getOrElse("StatsAggregation")
        RawStatsAggregationFilter(name, field)
    }

    //  def dHistAggregationFilter = log(histAggregationParser)("hist_agg")

    def histAggregationParser: Parser[RawHistogramAggregationFilter] = "type:hist" ~ nameParser.? ~ rawFieldParser ~
      intervalParser.? ~ minDocCountParser.? ~ extMinParser.? ~ extMaxParser.? ~ subAggregationsParser ^^ {
        case _ ~ nameOpt ~ field ~ intervalOpt ~ minDocCountOpt ~ extMinOpt ~ extMaxOpt ~ subAggs =>
          val name = nameOpt.getOrElse("HistogramAggregation")
          val interval = intervalOpt.getOrElse(5)
          val minDocCount = minDocCountOpt.getOrElse(0)
          RawHistogramAggregationFilter(name, field, interval, minDocCount, extMinOpt, extMaxOpt, subAggs)
      }

    def dSigTermAggParser = log(sigTermsAggregationParser)("sig_agg")

    def sigTermsAggregationParser: Parser[RawSignificantTermsAggregationFilter] = "type:sig" ~ nameParser.? ~ rawFieldParser ~
      backgroundTermParser.? ~ minDocCountParser.? ~ sizeParser.? ~ subAggregationsParser ^^ {
        case _ ~ nameOpt ~ field ~ backgroundTermOpt ~ minDocCountOpt ~ sizeOpt ~ subAggs =>
          val name = nameOpt.getOrElse("SignificantTermsAggregation")
          val minDocCount = minDocCountOpt.getOrElse(0)
          val size = sizeOpt.getOrElse(10)
          RawSignificantTermsAggregationFilter(name, field, backgroundTermOpt, minDocCount, size, subAggs)
      }

    def cardAggregationParser: Parser[RawCardinalityAggregationFilter] = "type:card" ~ nameParser.? ~ rawFieldParser ~ precisionThresholdParser.? ^^ {
      case _ ~ nameOpt ~ field ~ precisionThreshold =>
        val name = nameOpt.getOrElse("CardinalityAggregation")
        RawCardinalityAggregationFilter(name, field, precisionThreshold)
    }

    def parseAggregationParams(apString: Option[String]): Try[List[RawAggregationFilter]] = {
      if (apString.isDefined) {
        parseAll(aggregationsParser, apString.get) match {
          case Success(aggregationFilters, _) => scala.util.Success(aggregationFilters)
          case NoSuccess(msg, _) => scala.util.Failure(new IllegalArgumentException(msg))
        }
      } else {
        scala.util.Failure(new IllegalArgumentException("'ap' parameter is required for aggregation operation"))
      }
    }
  }

  /**
   * a parser for RTS rules.
   * syntax is:
   * /path/to/somewhere?qp=<operator><fields>
   * where is <operator> is one of: "*" (disregard the path, and apply query for every path),
   * "-" (apply for path, but NOT recursively)
   * <nothing> (apply for path recursively)
   * and <fields> is a sequence of comma (",") separated tuples,
   * where each tuple syntax is "<field-name>:<list-of-values>",
   * where <field-name> is a regular string,
   * and <list-of-values> can be one of: "[value1,value2,...,valueN]" (multiple values, separated by commas (",") and encapsulated in brackets ("[","]")
   * "some value" (a single value, no need for brackets)
   * <no-values> (when left empty, will search for every value for the field)
   * examples:
   *
   * /path/to/somewhere?qp=-f1:,f2:[v1,v2],f3:v3 # every value for field f1,
   * # values v1 and v2 for field f2,
   * # value v3 for field f3.
   * # but only for /path/to/somewhere and not for any descendants of it
   *
   * /path/to/somewhere?qp=*f1: # disregard the path,
   * # and query for every infoton containing the field f1
   *
   * /path/to/somewhere?qp=f1:[v1,v2,v3] # query every infoton that is either /path/to/somewhere,
   * # or any descendant of it for field f1 and values v1,v2,v3
   */
  object RTSQueryPredicate extends RegexParsers {

    import cmwell.rts.{ MatchFilter, MatchMap, NoFilter, PMFilter, Path, PathFilter, Rule }

    /* regex explanation:
     * value cannot contain the characters: ',' , '[' , ']' (used as separators)
     * field must start with a character and may contain alphanumeric characters, or '.', or '-'.
     */
    private val value: Parser[String] =
      """[^,\[\]]+""".r
    private val field: Parser[String] = """\w[\w\-.]*""".r //TODO: after ns-migration, this needs to be re-thought and change

    private def values: Parser[Seq[String]] = value ^^ (Seq(_)) | "[" ~> repsep(value, ",") <~ "]"

    private def single: Parser[(String, Seq[String])] = (field ~ ":" ~ values) ^^ {
      case f ~ ":" ~ v => f -> v
    }

    private def multi: Parser[Map[String, Seq[String]]] = repsep(single, ",") ^^ (_.toMap)

    private def rule: Parser[(String) => Rule] = opt("*" | "-") ~ multi ^^ {
      case Some("*") ~ m if m.isEmpty => _ => NoFilter
      case Some("-") ~ m if m.isEmpty => (path: String) => PathFilter(new Path(path, false))
      case None ~ m if m.isEmpty => (path: String) => if (path == "/") NoFilter else PathFilter(new Path(path, true))
      case Some("*") ~ m => _ => MatchFilter(new MatchMap(m.mapValues(_.map(FieldValue.parseString).toSet)))
      case Some("-") ~ m => (path: String) => PMFilter(new Path(path, false), new MatchMap(m.mapValues(_.map(FieldValue.parseString).toSet)))
      case None ~ m => (path: String) => PMFilter(new Path(path, true), new MatchMap(m.mapValues(_.map(FieldValue.parseString).toSet)))
    }

    def parseRule(qpString: String, path: String): Either[String, Rule] = {
      parseAll(rule, qpString) match {
        case Success(p2rule, _) => Right(p2rule(path))
        case NoSuccess(msg, _) => Left(msg)
      }
    }
  }

  class BaseExpandParser extends FieldFilterParser {

    //TODO: generalize to any RawFieldFilter. not just a flat single list
    protected val filter: Parser[RawFieldFilter] = "[" ~> fieldFilters <~ "]" ^? ({
      case sffs if sffs.size > 1 => RawMultiFieldFilter(Must, sffs)
      case List(sff) => sff
    }, xs => xs.headOption.fold("expansion filters must not be empty")(_ => s"unknown error for $xs"))

    protected val uriPattern: Parser[NsPattern] = "$" ~> namespaceUri <~ "$" ^? {
      case uri if uri.length > 1 && uri.takeRight(2).matches("(#|/|;)[*]") => NsUriPattern(uri.init)
    }

    protected val nsWildcardPattern: Parser[NsPattern] = uriPattern | ("[*][.]".r ~> fieldRegex ^^ {
      case s if s.head == '$' => HashedNsPattern(s.tail)
      case s => PrefixPattern(s)
    })

    protected val fieldPattern: Parser[FieldPattern] =
      (nsWildcardPattern ^^ NsWildCard.apply) | (fieldParser ^^ FieldKeyPattern.apply)

    protected def levelExpansionTautology(filter: Option[RawFieldFilter]): LevelExpansion =
      LevelExpansion(List(FilteredField(JokerPattern, filter)))

    protected def filteredField: Parser[FilteredField[FieldPattern]] = fieldPattern ~ filter.? ^^ {
      case p ~ f => FilteredField[FieldPattern](p, f)
    }

    protected def filteredFields = repsep(filteredField, ",")
  }

  /**
   * A parser for eXpandGraph API (xg).
   * syntax is:
   * xg={expander}{>expander}*
   * where `expander` is either a single wild card (`_`) which defaults to expanding every field (this is the default),
   * or a digit [1-9] which is equivalent to consecutive wild cards (in the amount specified by the digit),
   * or a plain field name with an optional (single) `*` wild card.
   * e.g: xg=2>*.vcard,employedBy.rel>_
   *
   * doctests:
   *
   * {{{
   * # Scala REPL style
   * scala> val fs = ExpandGraphParser.getLevelsExpansionFunctions("x>yyy>abc,xyz").get
   * fs: List[wsutil.LevelExpansion] = List(LevelExpansion(List(FilteredField(FieldKeyPattern(NnFieldKey(x)),None))), LevelExpansion(List(FilteredField(FieldKeyPattern(NnFieldKey(yyy)),None))), LevelExpansion(List(FilteredField(FieldKeyPattern(NnFieldKey(abc)),None), FilteredField(FieldKeyPattern(NnFieldKey(xyz)),None))))
   *
   * scala> val fs2 = ExpandGraphParser.getLevelsExpansionFunctions("x.$y").get
   * fs2: List[wsutil.LevelExpansion] = List(LevelExpansion(List(FilteredField(FieldKeyPattern(HashedFieldKey(x,y)),None))))
   *
   * scala> fs.size
   * res0: Int = 3
   *
   * scala> fs2.size
   * res1: Int = 1
   *
   * scala> ExpandGraphParser.getLevelsExpansionFunctions("_>_").get
   * res2: List[wsutil.LevelExpansion] = List(LevelExpansion(List(FilteredField(JokerPattern,None))), LevelExpansion(List(FilteredField(JokerPattern,None))))
   *
   * scala> ExpandGraphParser.getLevelsExpansionFunctions("2").get
   * res3: List[wsutil.LevelExpansion] = List(LevelExpansion(List(FilteredField(JokerPattern,None))), LevelExpansion(List(FilteredField(JokerPattern,None))))
   *
   * scala> (1 to 9).forAll { i =>
   *   val digits = ExpandGraphParser.getLevelsExpansionFunctions(i.toString).get
   *   val uscore = ExpandGraphParser.getLevelsExpansionFunctions(List.fill(i).mkString(">")).get
   *   digits == uscore
   * }
   * res3: Boolean = true
   *
   * scala> ExpandGraphParser.getLevelsExpansionFunctions("""belongsToDocket.JudicialMatter[type.rdf::http://ontology.thomsonreuters.com/legal/us/JudicialMatter/Motion,[*motionType.JudicialMatter::http://la.dev.metadata.thomsonreuters.com/us/JudicialMatter/docket/MotionType/motion-to-dismiss,*motionType.JudicialMatter::http://la.dev.metadata.thomsonreuters.com/us/JudicialMatter/docket/MotionType/motion-to-stay]]""").isSuccess
   * res4: Boolean = true
   * }}}
   *
   */
  object ExpandGraphParser extends BaseExpandParser {

    private def fields: Parser[LevelExpansion] = filteredFields ^^ LevelExpansion.apply

    protected def level: Parser[LevelExpansion] = {
      "_" ~> filter.? ^^ {
        case fOpt => levelExpansionTautology(fOpt)
      } | fields
    }

    private def levels: Parser[List[LevelExpansion]] = repsep(level, ">")

    private val digit: Parser[Int] = """[1-9]""".r ^^ { case s => s.toInt }

    private def jokers: Parser[List[LevelExpansion]] = digit ~ filter.? ^^ {
      case n ~ rffo => List.fill(n)(levelExpansionTautology(rffo))
    }

    private def expanders: Parser[List[LevelExpansion]] = (jokers ~ ">" ~ levels) ^^ {
      case js ~ ">" ~ ls => js ::: ls
    } | jokers | levels

    def getLevelsExpansionFunctions(xgInput: String): Try[List[LevelExpansion]] = {
      if (xgInput.isEmpty) USuccess(List(levelExpansionTautology(None)))
      else parseAll(expanders, xgInput) match {
        case Success(expns, _) => USuccess(expns)
        case NoSuccess(msg, _) => {
          val err = s"error: $msg, accured while parsing: $xgInput"
          val ex = new IllegalArgumentException(err)
          logger.error(err, ex)
          scala.util.Failure(ex)
        }
      }
    }
  }

  /**
   * yg parser
   * TODO: write yg syntax documentation
   *
   * {{{
   * # Scala REPL style
   * scala> PathGraphExpansionParser.getPathsExpansionFunctions("""<belongsToDocket.JudicialMatter[type.rdf::http://ontology.thomsonreuters.com/legal/us/JudicialMatter/Motion,[*motionType.JudicialMatter::http://la.dev.metadata.thomsonreuters.com/us/JudicialMatter/docket/MotionType/motion-to-dismiss,*motionType.JudicialMatter::http://la.dev.metadata.thomsonreuters.com/us/JudicialMatter/docket/MotionType/motion-to-stay]]""").isSuccess
   * res0: Boolean = true
   * }}}
   */
  object PathGraphExpansionParser extends BaseExpandParser {

    private def filteredFieldExact: Parser[FilteredField[FieldKeyPattern]] = (fieldParser ^^ FieldKeyPattern.apply) ~ filter.? ^^ {
      case fkp ~ f => FilteredField[FieldKeyPattern](fkp, f)
    }
    private def filteredFieldsExact = repsep(filteredFieldExact, ",")

    private def expandUp: Parser[ExpandUp] = "<" ~> filteredFieldsExact ^^ ExpandUp.apply

    private def expandIn: Parser[ExpandIn] = ">" ~> filteredFields ^^ ExpandIn.apply

    private def path: Parser[PathExpansion] = rep(expandIn | expandUp) ^^ PathExpansion.apply

    private def paths: Parser[PathsExpansion] = repsep(path, "|") ^^ PathsExpansion.apply

    def getPathsExpansionFunctions(ygInput: String): Try[PathsExpansion] = {
      if (ygInput.isEmpty) scala.util.Failure(new IllegalArgumentException("yg empty input"))
      else {
        parseAll(paths, ygInput) match {
          case Success(functions, _) => USuccess(functions)
          case NoSuccess(msg, _) => scala.util.Failure(new IllegalArgumentException(s"error: $msg, accured while parsing: $ygInput"))
        }
      }
    }
  }

  class BaseFieldFilterParser extends RegexAndFieldNameParser {

//    TODO: verify that new regex accept everything old regex accepted
//    def literalParser: Parser[String] = """(?:[\p{L}\p{Sc}\(\)_0-9/.@ &:\-\\']+)""".r

    val unescapedValue = """[^:<>$,\]][^,\]]*""".r
      .withFailureMessage("A value cannot start with any of ':','<','>','$',',',']' nor it can contain commas or closing brackets (',',']'). " +
        "In case you need to match these characters, use dollar escaped expression (e.g: using \"$<foo,$$,bar>$\" will match \"<foo,$,bar>\").")

    val dollarsEscaped = ("$" ~> commit("""(?:(?:\$\$)|[^$])+""".r <~ "$") ^^ (_.replaceAll("\\Q$$\\E","\\$")))
      .withFailureMessage("Dollars escaped strings can match any string, as long that any '$' character is double escaped " +
        "(e.g: to match \"foo$bar\" you'll need to query for: \"foo$$bar\"), and the entire expression should be  wrapped with single dollar characters (i.e: \"$foo$$bar$\").")

    def valueParser: Parser[Option[String]] = (dollarsEscaped | unescapedValue).?

    def singleFieldFilters: Parser[List[RawFieldFilter]] = rep1sep(singleFieldFilter, ",")

    def singleFieldFilter: Parser[RawSingleFieldFilter] = fieldOperator ~ fieldParser ~ valueOperator ~ valueParser ^^ {
      case fieldOperator ~ fieldKey ~ valueOperator ~ value => RawSingleFieldFilter(fieldOperator, valueOperator, fieldKey, value)
    }

    def fieldOperator: Parser[FieldOperator] = opt("-" | "*") ^^ {
      case Some("-") => MustNot
      case Some("*") => Should
      case _ => Must
    }

    def valueOperator: Parser[ValueOperator] = ("::" | ":" | ">>" | ">" | "<<" | "<" | "~") ^^ {
      case "::" => Equals
      case ":" => Contains
      case ">>" => GreaterThanOrEquals
      case ">" => GreaterThan
      case "<<" => LessThanOrEquals
      case "<" => LessThan
      case "~" => Like
    }
  }

  /**
   * search's qp parser
   * TODO: write syntax documentation
   *
   * Adding some tests to the regex
   * doctests:
   *
   * {{{
   * # Scala REPL style
   * scala> val drDansQuery = FieldFilterParser.parseQueryParams("""system.quad::http://data.thomsonreuters.com/2-667823""")
   * drDansQuery: scala.util.Try[wsutil.RawFieldFilter] = Success(RawSingleFieldFilter(Must,Equals,NnFieldKey(system.quad),Some(http://data.thomsonreuters.com/2-667823)))
   *
   * scala> drDansQuery.isSuccess
   * res0: Boolean = true
   *
   * scala> val bogusQuery = FieldFilterParser.parseQueryParams("""x:,:z""")
   * bogusQuery: scala.util.Try[wsutil.RawFieldFilter] = Failure(cmwell.web.ld.exceptions.ParsingException: string matching regex `[\w+\-*.$]+' expected but `:' found)
   *
   * scala> bogusQuery.isSuccess
   * res1: Boolean = false
   *
   * scala> val bogusQuery2 = FieldFilterParser.parseQueryParams("""x:~,~:z""")
   * bogusQuery: scala.util.Try[wsutil.RawFieldFilter] = Failure(cmwell.web.ld.exceptions.ParsingException: string matching regex `[\w+\-*.$]+' expected but `:' found)
   *
   * scala> bogusQuery2.isSuccess
   * res2: Boolean = false
   *
   * scala> val dollarEscaped = FieldFilterParser.parseQueryParams("""x:$~,~:z$""")
   * dollarEscaped: scala.util.Try[wsutil.RawFieldFilter] = Success(RawSingleFieldFilter(Must,Contains,NnFieldKey(x),Some(~,~:z)))
   *
   * scala> dollarEscaped.isSuccess
   * res3: Boolean = true
   *
   * scala> val davidSeamanQuery = FieldFilterParser.parseQueryParams("""organizationNameLocalLng.fedapioa:บริษัท+ทิมบลิค+แอนด์+พาร์ทเนอร์ส+จำกัด""")
   * davidSeamanQuery: scala.util.Try[wsutil.RawFieldFilter] = Success(RawSingleFieldFilter(Must,Contains,PrefixFieldKey(organizationNameLocalLng,fedapioa),Some(บริษัท+ทิมบลิค+แอนด์+พาร์ทเนอร์ส+จำกัด)))
   *
   * scala> davidSeamanQuery.isSuccess
   * res4: Boolean = true
   * }}}
   *
   */
  class FieldFilterParser extends BaseFieldFilterParser {

    def multiFieldFilter: Parser[RawMultiFieldFilter] = fieldOperator ~ "[" ~ fieldFilters <~ "]" ^^ {
      case fo ~ _ ~ ffs => RawMultiFieldFilter(fo, ffs)
    }

    def rawFieldFilter: Parser[RawFieldFilter] = multiFieldFilter | singleFieldFilter

    def fieldFilters: Parser[Seq[RawFieldFilter]] = repsep(rawFieldFilter,",")

    def unwrappedFieldFilters: Parser[RawFieldFilter] = fieldFilters ^^ {
      case xs if xs.size == 1 => xs.head
      case xs => RawMultiFieldFilter(Must, xs)
    }

    def parseQueryParams(qpString: String): Try[RawFieldFilter] = {

      //TODO: should be used at evaluation stage
      def mapQuads(fieldFilter: RawFieldFilter): RawFieldFilter = {
        fieldFilter match {
          case RawMultiFieldFilter(fo, filters) => RawMultiFieldFilter(fo, filters.map(mapQuads))
          case RawSingleFieldFilter(fo, vo, Right(NnFieldKey("system.quad")), Some(quad)) if !FReference.isUriRef(quad) => {
            UnevaluatedQuadFilter(fo,vo,quad)
          }
          case rff => rff
        }
      }

      if (qpString.isEmpty) UFailure(new IllegalArgumentException("qp param must not be empty"))
      else parseAll(unwrappedFieldFilters, qpString) match {
        case Success(rff, _) => Try(mapQuads(rff))
        case NoSuccess(msg, _) => UFailure(new IllegalArgumentException(msg))
      }
    }
  }

  object FieldFilterParser {
    private[this] val parser = new FieldFilterParser

    def parseQueryParams(qpString: String): Try[RawFieldFilter] =
      parser.parseQueryParams(qpString)
  }

  object SortedIteratorIdParser extends FieldFilterParser {

    val timestamp: Parser[Long] = wholeNumber ^^ { _.toLong }

    val from: Parser[Option[DateTime]] = opt(wholeNumber) ^^ {
      _.map(n => new DateTime(n.toLong))
    }

    val to: Parser[Option[DateTime]] = opt(wholeNumber) ^^ {
      _.map(n => new DateTime(n.toLong))
    }

    val path: Parser[Option[String]] = opt("[^|$]+".r)

    val descendants: Parser[Boolean] = opt("d" | "r") ^^ {
      _.isDefined
    }

    val history: Parser[Boolean] = opt("h") ^^ {
      _.isDefined
    }

    val properties = from ~ "|" ~ to ~ "|" ~ path ~ "|" ~ descendants ~ "|" ~ history ^^ {
      case f ~ _ ~ t ~ _ ~ p ~ _ ~ d ~ _ ~ h => (f, t, p, d, h)
    }

    val iteratorId = timestamp ~ "|" ~ properties ~ "|" ~ ".*".r ^^ {
      case ts ~ _ ~ ((f, t, p, d, h)) ~ _ ~ qp =>
        if (qp.isEmpty) USuccess(SortedIteratorState(ts, None, f, t, p, d, h))
        else FieldFilterParser.parseQueryParams(qp).flatMap(rff => {
          extractRawKeyAsFieldName(rff).map { ff =>
            SortedIteratorState(ts, Some(ff), f, t, p, d, h)
          }
        })
    }

    def extractRawKeyAsFieldName(rff: RawFieldFilter): Try[FieldFilter] = rff match {
      case RawMultiFieldFilter(fo, rffs) => Try.traverse(rffs)(extractRawKeyAsFieldName).map(MultiFieldFilter(fo, _))
      case RawSingleFieldFilter(fo, vo, Right(dfk), v) => USuccess(SingleFieldFilter(fo, vo, dfk.internalKey, v))
      case UnevaluatedQuadFilter(_,_,alias) => UFailure {
         new IllegalArgumentException(s"supplied fields must be direct. system.quad with alias[$alias] is not direct and needs to be resolved (use fully qualified URI instead).")
      }
      case RawSingleFieldFilter(fo, vo, Left(rfk), v) => UFailure {
        new IllegalArgumentException(s"supplied fields must be direct. ${rfk.externalKey} needs to be resolved.")
      }
    }

    def parseSortedIteratorId(base64EncodedId: String) = {
      if (base64EncodedId.isEmpty) scala.util.Failure(new IllegalArgumentException("position cannot be empty"))
      else Try {
        val bArr = cmwell.util.string.Base64.decodeBase64(base64EncodedId)
        cmwell.util.string.Zip.decompress(bArr)
      }.flatMap { uuidsPropsAndQp =>
        parseAll(iteratorId, uuidsPropsAndQp) match {
          case Success(idTry, _) => idTry
          case NoSuccess(msg, _) => scala.util.Failure(new IllegalArgumentException(msg))
        }
      }
    }
  }

  object SortedIteratorIdFormatter {

    def formatSortedIteratorId(sortedIteratorState: SortedIteratorState): String = {
      val SortedIteratorState(ts, qp, f, t, p, d, h) = sortedIteratorState
      val parts = List(
        ts.toString,
        f.map(_.getMillis.toString).getOrElse(""),
        t.map(_.getMillis.toString).getOrElse(""),
        p.getOrElse(""),
        if (d) "d" else "",
        if (h) "h" else "",
        qp.fold("")(cmwell.ws.qp.Encoder.encodeFieldFilter)
      )
      val bare = parts.mkString("|")
      val bArr = cmwell.util.string.Zip.compress(bare)
      cmwell.util.string.Base64.encodeBase64URLSafeString(bArr)
    }
  }

  /**
   * sort-by parser
   * TODO: write syntax documentation
   */
  object SortByParser extends RegexAndFieldNameParser {

    def fieldSortOperator: Parser[FieldSortOrder] = opt("-" | "*") ^^ {
      case Some("-") => Desc
      case Some("*") => Asc
      case _ => Asc
    }

    def fieldSortParam: Parser[RawSortParam.RawFieldSortParam] = fieldSortOperator ~ fieldParser ^^ {
      case order ~ fieldKey => fieldKey -> order
    }

    def fieldSortParams: Parser[RawSortParam] = rep1sep(fieldSortParam, ",") ^^ RawFieldSortParam.apply

    def parseFieldSortParams(fsParams: String): Try[RawSortParam] = {

      if (fsParams.isEmpty || fsParams.equalsIgnoreCase("system.score")) USuccess(RawNullSortParam)
      else parseAll(fieldSortParams, fsParams) match {
        case Success(fieldSortParams, _) => USuccess(fieldSortParams)
        case NoSuccess(msg, _) => scala.util.Failure(new IllegalArgumentException(msg))
      }
    }
  }

  trait DateType

  case object FromDate extends DateType

  case object ToDate extends DateType

  object DateParser extends LazyLogging {
    val fullDateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)
    val fdf = fullDateFormatter.print(_: DateTime)
    val longDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val shortDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

    def parseDate(dateStr: String, dateType: DateType): Try[DateTime] = {
      try {
        scala.util.Success(fullDateFormatter.parseDateTime(dateStr))
      } catch {
        case _: Throwable =>
          try {
            scala.util.Success(longDateFormatter.parseDateTime(dateStr))
          } catch {
            case _: Throwable =>
              try {
                val dateTime = shortDateFormatter.parseDateTime(dateStr).toMutableDateTime
                dateType match {
                  case FromDate => dateTime.setTime(0, 0, 0, 0)
                  case ToDate => dateTime.setTime(23, 59, 59, 999)
                }
                scala.util.Success(dateTime.toDateTime)
              } catch {
                case _: Throwable =>
                  logger.debug("date format invalid: " + dateStr);
                  dateType match {
                    case FromDate => scala.util.Failure(new IllegalArgumentException("Invalid Parameter: Invalid 'From' date pattern. Use either: yyyy-MM-dd'T'HH:mm:ss.SSS'Z', yyyy-MM-dd HH:mm:ss or yyyy-MM-dd"))
                    case ToDate => scala.util.Failure(new IllegalArgumentException("Invalid Parameter: Invalid 'To' date pattern. Use either: yyyy-MM-dd'T'HH:mm:ss.SSS'Z', yyyy-MM-dd HH:mm:ss or yyyy-MM-dd"))
                  }
              }
          }
      }
    }
  }

}