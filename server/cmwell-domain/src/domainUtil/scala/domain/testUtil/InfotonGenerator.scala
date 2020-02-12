
/**
 * Created by markz on 10/20/14.
 */

package domain.testUtil

import cmwell.domain._
import org.joda.time.{DateTime, DateTimeZone}
import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import scala.util.Random

object InfotonGenerator {

  val currTime = new DateTime(DateTimeZone.UTC)
  val currTimeMillies = currTime.getMillis

  def genericSystemFields =
  {
    def createGenSysfields = {
      for {
        path <- Gen.resize(7, Gen.nonEmptyListOf[String](Gen.resize(5, Gen.identifier)))
        lastModifiedBy <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier))
        dc <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier))
        indexName <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier))
        indexTime <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Long](Gen.choose(currTimeMillies, currTimeMillies+10000L)))
      } yield {
        val rnd = new Random
        val indexTimeOp = Seq(None, Some(indexTime.head))
        val protocolOp = Seq("http", "https")

        SystemFields(
          path.mkString("/", "/", ""),
          new DateTime(DateTimeZone.UTC),
          lastModifiedBy.head,
          dc.head,
          indexTimeOp(rnd.nextInt(indexTimeOp.size)),
          indexName.head,
          protocolOp(rnd.nextInt(protocolOp.size))
        )
      }
    }

    val genSysFields = createGenSysfields
    val sysFields = genSysFields.apply(Gen.Parameters.default.withSize(Random.nextInt(2)), Seed.random())
    sysFields.get
  }

  /*def infotonGen = {
    val infotons: Gen[Infoton] = for {
      iType <- Gen.choose(0, 1) //TODO: extend to (0,2) to have also LinkInfoton generation
      path <- Gen.resize(7, Gen.nonEmptyListOf[String](Gen.resize(5, Gen.identifier)))
      fields <- Gen.resize(4, Gen.nonEmptyListOf[String](Gen.resize(5, Gen.identifier)))
      strVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier))
      intVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Int](Gen.choose(-100, 100)))
      lngVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Long](Gen.choose(-100L, 100L)))
      numVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, java.math.BigInteger](Gen.choose(-100, 100).map(BigInt(_).underlying)))
      fltVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Float](Gen.choose(-100.0f, 100.0f)))
      dblVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Double](Gen.choose(-100.0, 100.0)))
      decVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, java.math.BigDecimal](Gen.choose(-100.0, 100.0).map(BigDecimal(_).underlying)))
      txtVal <- Gen.resize(50, Gen.identifier)
      blnVal <- Gen.oneOf(true, false)
      //    binVal <- Gen.resize(50, Gen.nonEmptyContainerOf[Array,Byte](Gen.choose(0.toByte,255.toByte)))
      //    extVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier)) //TODO: FExtenal, FDate, FReference
    } yield {
      def mkFields = {
        val m = scala.collection.mutable.Map[String, Set[FieldValue]]()
        fields.foreach(f => scala.util.Random.nextInt(8) match {
          case 0 => m.update(f, strVals.map(FString(_)))
          case 1 => m.update(f, intVals.map(FInt(_)))
          case 2 => m.update(f, lngVals.map(FLong(_)))
          case 3 => m.update(f, numVals.map(FBigInt(_)))
          case 4 => m.update(f, fltVals.map(FFloat(_)))
          case 5 => m.update(f, dblVals.map(FDouble(_)))
          case 6 => m.update(f, decVals.map(FBigDecimal(_)))
          case 7 => m.update(f, Set(FBoolean(blnVal)))
          case _ => ??? //TODO: add FDate, FExternal, FReference
        })
        Map[String, Set[FieldValue]]() ++ m
      }


      val systemFields = genericSystemFields

      iType match {
        case 0 => ObjectInfoton(systemFields, mkFields)
        case 1 => {
          val (content, mimeType): Tuple2[Array[Byte], String] = scala.util.Random.nextBoolean() match {
            case true => (txtVal.getBytes("UTF-8"), "text/plain")
            case false => {
              val rSize = 50 + scala.util.Random.nextInt(100)
              val iArr = (1 to rSize).map(_ => scala.util.Random.nextInt(256))
              (iArr.map(_.toByte).toArray, "application/octet-stream")
            }
          }
          val f = scala.util.Random.nextBoolean() match {
            case false => None
            case true => Some(mkFields)
          }
          FileInfoton(systemFields, fields = f, content = Some(FileContent(content, mimeType)))
        }
        case 2 => ??? //unreacable for now, TODO: add LinkInfoton Generation
        case _ => ??? //should never get here
      }
    }

    val infoton = infotonGen.apply(Gen.Parameters.default.withSize(Random.nextInt(2)), Seed.random())
    infoton.get
  }*/

  val sCmp: Function2[Infoton,Infoton,Boolean] = (i,j) =>  {
    i.systemFields.path == j.systemFields.path && {
      List(i.systemFields.lastModified, j.systemFields.lastModified).size match {
        case 0 => true
        case 1 => false
        case _ => i.systemFields.lastModified.compareTo(j.systemFields.lastModified) == 0
      }
    } && {
      List(i.uuid, j.uuid).flatten.size match {
        case 0 => true
        case 1 => false
        case _ => i.uuid == j.uuid
      }
    } && {
      List(i.parent,j.parent).flatten.size match {
        case 0 => true
        case 1 => false
        case _ => i.parent == j.parent
      }
    }
  }

  val fCmp: Function2[Option[Map[String, Set[FieldValue]]],Option[Map[String, Set[FieldValue]]],Boolean] = (i,j) => List(i,j).flatten.size match {
    case 0 => true
    case 1 => false
    case _ => {
      val m1 = i.get
      val m2 = j.get
      m1.keySet == m2.keySet && m1.forall{
        case (k,v) => m2(k) == v
      }
    }
  }

  val cCmp: Function2[Option[FileContent],Option[FileContent],Boolean] = (i,j) => List(i,j).flatten.size match {
    case 0 => true
    case 1 => false
    case _ => i.get == j.get
  }

  val iCmp: Function2[Infoton,Infoton,Boolean] = (i,j) => (i,j) match {
    case (i:ObjectInfoton, j:ObjectInfoton) => sCmp(i,j) && fCmp(i.fields, j.fields)
    case (f1:FileInfoton, f2:FileInfoton) => sCmp(f1,f2) && fCmp(f1.fields,f2.fields) && cCmp(f1.content,f2.content)
    case (l1:LinkInfoton, l2:LinkInfoton) => sCmp(l1, l2) && l1.linkTo == l2.linkTo && l1.linkType == l2.linkType
    case _ => false
  }
}
