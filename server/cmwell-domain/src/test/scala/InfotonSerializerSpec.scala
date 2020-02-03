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


import cmwell.domain._
import domain.testUtil.InfotonGenerator.genericSystemFields
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

import scala.language.postfixOps

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 1/13/13
 * Time: 3:15 PM
 * Testing infoton serializer code
 */

class InfotonSerializerSpec extends FlatSpec with Matchers {

  def serialize2Anddeserialize2(i: Infoton): Infoton = {
    val (uuid,rows) = InfotonSerializer.serialize2(i)
    val it = rows.view.flatMap {
      case (q,fields) => fields.view.sortBy(_._1).flatMap{
        case (fieldName,values) =>  values.view.sortBy(_._1).map(value => (q,fieldName,value))
      }
    }.iterator
    InfotonSerializer.deserialize2(uuid,it)
  }

  "very big infoton" should "be successful" in {

    val x: Set[FieldValue] = {
      val b = Set.newBuilder[FieldValue]
      b.sizeHint(100000)
      for(i <- 0 until 100000) {
        b += FString(s"cmwell://name/$i")
      }
      b.result()
    }

    val objInfo = new ObjectInfoton(genericSystemFields, Option(Map[String,Set[FieldValue]]("g" -> Set(FString("h")),"last" -> Set(FString("zitnik")) )))
    serialize2Anddeserialize2(objInfo) shouldEqual objInfo
  }

  "object infoton serializer" should "be successful" in {
    val objInfo = ObjectInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))))
    val objInfoCmp = serialize2Anddeserialize2(objInfo)
    // check system
    objInfo.systemFields.path should equal(objInfoCmp.systemFields.path)
    objInfo.uuid should equal(objInfoCmp.uuid)
    objInfo.systemFields.lastModified should equal(objInfoCmp.systemFields.lastModified)
    objInfo.systemFields.lastModifiedBy should equal(objInfoCmp.systemFields.lastModifiedBy)
    // check fields
    objInfo.fields.get("name").size should equal(objInfoCmp.fields.get("name").size)
  }

  "empty file infoton serializer" should "be successful" in {
    val fc = FileContent("text/plain",0)
    val emptyInfo = FileInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , fc)
    val emptyInfoCmp = serialize2Anddeserialize2(emptyInfo)

    emptyInfo.systemFields.path should equal (emptyInfoCmp.systemFields.path)
    emptyInfo.uuid should equal (emptyInfoCmp.uuid)
    emptyInfo.systemFields.lastModified should equal (emptyInfoCmp.systemFields.lastModified)
    emptyInfo.systemFields.lastModifiedBy should equal(emptyInfoCmp.systemFields.lastModifiedBy)

  }

  "file binary infoton serializer" should "be successful" in {

    val source = scala.io.Source.fromFile("./src/test/resources/mascot.jpg" ,"iso-8859-1")
    val byteArray = source.map(_.toByte).toArray
    source.close()

    val s = byteArray
    val img : FileContent = FileContent(s, "image/jpeg;charset=iso-8859-1")
    val imgInfo = FileInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))), img)
    val imgInfoCmp = serialize2Anddeserialize2(imgInfo)

    // check system
    imgInfo.systemFields should equal (imgInfoCmp.systemFields)
    imgInfo.uuid should equal (imgInfoCmp.uuid)

    // check fields
    imgInfo.fields.get("name").size should equal (imgInfoCmp.fields.get("name").size)

    (imgInfoCmp: @unchecked) match {
      case FileInfoton(_, _, content) =>
        content.get match {
          case FileContent(data,mimeType,_,_) =>
            val d = data.get
            d should equal (s)
            "image/jpeg;charset=iso-8859-1" should equal (mimeType)
      }
    }
  }

  "file text infoton serializer" should "be successful" in {

    val source = scala.io.Source.fromFile("./src/test/resources/test.txt" ,"UTF-8")
    val byteArray = source.map(_.toByte).toArray
    source.close()

    val s = byteArray
    val text : FileContent = FileContent(s, "text/plain;charset=utf-8")
    val textInfo = FileInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))), text)
    val textInfoCmp = serialize2Anddeserialize2(textInfo)

    // check system
    textInfo.systemFields should equal (textInfoCmp.systemFields)
    textInfo.uuid should equal (textInfoCmp.uuid)

    // check fields
    textInfo.fields.get("name").size should equal (textInfoCmp.fields.get("name").size)

    (textInfoCmp: @unchecked) match {
      case FileInfoton(_,_, content) =>
        content.get match {
          case FileContent(data,mimeType,_,_) =>
            val d = data.get
            d should equal (s)
            "text/plain;charset=utf-8" should equal (mimeType)
      }
    }
  }

  // TODO: make this configurable
  val chunkSize = 65536

  "big file infoton with % chunkSize != 0" should "be successful" in {
    val bArr = Array.tabulate[Byte](chunkSize + chunkSize + 12345)(_.&(0xff).toByte)
    val data : FileContent = FileContent(bArr, "application/octet-stream")
    val fInf = FileInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))), data)
    val dataInfoCmp = serialize2Anddeserialize2(fInf)

    // check system
    fInf.systemFields should equal (dataInfoCmp.systemFields)
    fInf.uuid should equal (dataInfoCmp.uuid)

    // check fields
    fInf.fields.get("name").size should equal (dataInfoCmp.fields.get("name").size)

    (dataInfoCmp: @unchecked) match {
      case FileInfoton(_,_,content) =>
        content.get match {
          case FileContent(binData,mimeType,_,_) =>
            val d = binData.get
            d should equal (bArr)
            "application/octet-stream" should equal (mimeType)
      }
    }
  }

  "big file infoton with % chunkSize == 0" should "be successful" in {
    val bArr = Array.tabulate[Byte](2*chunkSize)(_.&(0xff).toByte)
    val data : FileContent = FileContent(bArr, "application/octet-stream")
    val fInf = FileInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))), data)
    val dataInfoCmp = serialize2Anddeserialize2(fInf)

    // check system
    fInf.systemFields should equal (dataInfoCmp.systemFields)
    fInf.uuid should equal (dataInfoCmp.uuid)

    // check fields
    fInf.fields.get("name").size should equal (dataInfoCmp.fields.get("name").size)
    (dataInfoCmp: @unchecked) match {
      case FileInfoton(_,_,content) =>
        content.get match {
          case FileContent(binData,mimeType,_,_) =>
            val d = binData.get
            d should equal (bArr)
            "application/octet-stream" should equal (mimeType)
        }
    }
  }

  "link infoton serializer" should "be successful" in {
    val forward = LinkInfoton(genericSystemFields,
      Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))),
      "/mark",
      LinkType.Forward)

    val forwardCmp = serialize2Anddeserialize2(forward)
    // check system
    forward.systemFields should equal (forwardCmp.systemFields)
    forward.uuid should equal (forwardCmp.uuid)

    // check link
    (forwardCmp: @unchecked) match {
      case LinkInfoton(_, _,t,lt) => {
        forward.linkTo should equal (t)
        forward.linkType should equal (lt)
      }
    }
    // check fields
    forward.fields.get("name").size should equal (forwardCmp.fields.get("name").size)

    val per = LinkInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Permanent)

    val perCmp = serialize2Anddeserialize2(per)
    // check system
    per.systemFields should equal (perCmp.systemFields)
    per.uuid should equal (perCmp.uuid)

    // check link
    (perCmp: @unchecked) match {
      case LinkInfoton(_,_,t,lt) => {
        per.linkTo should equal (t)
        per.linkType should equal (lt)
      }
    }
    // check fields
    per.fields.get("name").size should equal (perCmp.fields.get("name").size)


    val temp = LinkInfoton(genericSystemFields, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Temporary)

    val tempCmp =serialize2Anddeserialize2(temp)
    // check system
    temp.systemFields should equal (tempCmp.systemFields)
    temp.uuid should equal (tempCmp.uuid)

    // check link
    (tempCmp: @unchecked) match {
      case LinkInfoton(_,_,t,lt) => {
        temp.linkTo should equal (t)
        temp.linkType should equal (lt)
      }
    }
    // check fields
    temp.fields.get("name").size should equal (tempCmp.fields.get("name").size)

  }

  "deleted infoton serializer" should "be successful" in {
    val deleted = DeletedInfoton(genericSystemFields)
    val deletedCmp = serialize2Anddeserialize2(deleted)

    deleted.systemFields should equal (deletedCmp.systemFields)
  }

  "diffetent infotons with same fields" should "return isSameAs==true" in {
    val systemFields = genericSystemFields
    val fieldsMap = Map("Mark"->Set[FieldValue](FString("King"),FString("Awesome")))
    val infoton1 = ObjectInfoton(systemFields, fieldsMap)
    val infoton2 = ObjectInfoton(systemFields.copy(path = "/pathOfInfoton2", lastModified = new DateTime("2001-02-03T09:34:21.000Z")), fieldsMap)
    (infoton1 isSameAs infoton2) should equal (true)
  }

}
