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


package cmwell.common

import org.scalacheck._
import Prop.forAll
import cmwell.domain._
import domain.testUtil.InfotonGenerator.genericSystemFields

/**
 * Created by gilad on 7/29/14.
 */
object InfotonSerializationSpecification extends Properties("Infoton") {

  val infotons: Gen[Infoton] = for {
    iType <- Gen.choose(0,1) //TODO: extend to (0,2) to have also LinkInfoton generation
    path <- Gen.resize(7, Gen.nonEmptyListOf[String](Gen.resize(5, Gen.identifier)))
    fields <- Gen.resize(4, Gen.nonEmptyListOf[String](Gen.resize(5, Gen.identifier)))
    strVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Arbitrary.arbitrary[String]))
    engVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier))
    intVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Int](Gen.choose(-100, 100)))
    lngVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Long](Gen.choose(-100L, 100L)))
    numVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, java.math.BigInteger](Gen.choose(-100,100).map(BigInt(_).underlying)))
    fltVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Float](Gen.choose(-100.0f, 100.0f)))
    dblVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, Double](Gen.choose(-100.0, 100.0)))
    decVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, java.math.BigDecimal](Gen.choose(-100.0, 100.0).map(BigDecimal(_).underlying)))
    txtVal <- Gen.resize(50, Gen.identifier)
    blnVal <- Gen.oneOf(true,false)
  //    binVal <- Gen.resize(50, Gen.nonEmptyContainerOf[Array,Byte](Gen.choose(0.toByte,255.toByte)))
  //    extVals <- Gen.resize(4, Gen.nonEmptyContainerOf[Set, String](Gen.identifier)) //TODO: FExtenal, FDate, FReference
  } yield {
    def mkFields = {
      val m = scala.collection.mutable.Map[String, Set[FieldValue]]()
      fields.foreach(f => scala.util.Random.nextInt(9) match {
        case 0 => m.update(f,engVals.map(FString(_)))
        case 1 => m.update(f,intVals.map(FInt(_)))
        case 2 => m.update(f,lngVals.map(FLong(_)))
        case 3 => m.update(f,numVals.map(FBigInt(_)))
        case 4 => m.update(f,fltVals.map(FFloat(_)))
        case 5 => m.update(f,dblVals.map(FDouble(_)))
        case 6 => m.update(f,decVals.map(FBigDecimal(_)))
        case 7 => m.update(f,Set(FBoolean(blnVal)))
        case 8 => m.update(f,strVals.map(FString(_)))
        case _ => ??? //TODO: add FDate, FExternal, FReference
      })
      Map[String,Set[FieldValue]]() ++ m
    }
    iType match {
      case 0 => ObjectInfoton(genericSystemFields.copy(path = path.mkString("/", "/", "")), mkFields)
      case 1 => {
        val (content, mimeType): Tuple2[Array[Byte],String] = scala.util.Random.nextBoolean() match{
          case true => (txtVal.getBytes("UTF-8"),"text/plain")
          case false => {
            val rSize = 50 + scala.util.Random.nextInt(100)
            val iArr = (1 to rSize).map(_ => scala.util.Random.nextInt(256))
            (iArr.map(_.toByte).toArray,"application/octet-stream")
          }
        }
        val f = scala.util.Random.nextBoolean() match {
          case false => None
          case true => Some(mkFields)
        }
        FileInfoton(genericSystemFields.copy(path = path.mkString("/", "/", "")), f, Some(FileContent(content, mimeType)))
      }
      case 2 => ??? //unreacable for now, TODO: add LinkInfoton Generation
      case _ => ??? //should never get here
    }
  }

  val sCmp: Function2[Infoton,Infoton,Boolean] = (i,j) =>  {
    i.systemFields.path == j.systemFields.path &&
    i.systemFields.lastModified.compareTo(j.systemFields.lastModified) == 0 &&
    i.uuid == j.uuid &&
    i.parent == j.parent
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

  def serialize2Anddeserialize2(i: Infoton): Infoton = {
    val (uuid,rows) = InfotonSerializer.serialize2(i)
    val it = rows.view.flatMap {
      case (q,fields) => fields.view.sortBy(_._1).flatMap{
        case (fieldName,values) =>  values.view.sortBy(_._1).map(value => (q,fieldName,value))
      }
    }.iterator
    InfotonSerializer.deserialize2(uuid,it)
  }

  property("serializeAndDeserialize") = forAll(infotons) {
    i: Infoton => {
      val d = serialize2Anddeserialize2(i)
      iCmp(i,d)
    }
  }
}
