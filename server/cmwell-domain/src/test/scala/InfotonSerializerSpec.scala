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


import cmwell.domain._
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

    val objInfo = new ObjectInfoton(path = "/command-test/objinfo1", dc = "test", fields = Option(Map[String,Set[FieldValue]]("g" -> Set(FString("h")),"last" -> Set(FString("zitnik")) , "name" ->  x )))
    InfotonSerializer.deserialize(InfotonSerializer.serialize(objInfo)) shouldEqual objInfo
    serialize2Anddeserialize2(objInfo) shouldEqual objInfo
  }

  "object infoton serializer" should "be successful" in {
    val objInfo = ObjectInfoton("/command-test/objinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))))
    val objInfoCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(objInfo))
    // check system
    objInfo.path should equal(objInfoCmp.path)
    objInfo.uuid should equal(objInfoCmp.uuid)
    objInfo.lastModified should equal(objInfoCmp.lastModified)
    // check fields
    objInfo.fields.get("name").size should equal(objInfoCmp.fields.get("name").size)
  }

  "empty file infoton serializer" should "be successful" in {
    val fc = FileContent("text/plain",0)
    val emptyInfo = FileInfoton("/command-test/objinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , fc )
    val emptyInfoCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(emptyInfo))

    emptyInfo.path should equal (emptyInfoCmp.path)
    emptyInfo.uuid should equal (emptyInfoCmp.uuid)
    emptyInfo.lastModified should equal (emptyInfoCmp.lastModified)

  }

  "file binary infoton serializer" should "be successful" in {

    val source = scala.io.Source.fromFile("./src/test/resources/mascot.jpg" ,"iso-8859-1")
    val byteArray = source.map(_.toByte).toArray
    source.close()

    val s = byteArray
    val img : FileContent = FileContent(s, "image/jpeg;charset=iso-8859-1")
    val imgInfo = FileInfoton("/command-test/objinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , img )
    val imgInfoCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(imgInfo))
    val imgInfoCmp2 = serialize2Anddeserialize2(imgInfo)

    // check system
    imgInfo.path should equal (imgInfoCmp.path)
    imgInfo.uuid should equal (imgInfoCmp.uuid)
    imgInfo.lastModified should equal (imgInfoCmp.lastModified)
    imgInfo.path should equal (imgInfoCmp2.path)
    imgInfo.uuid should equal (imgInfoCmp2.uuid)
    imgInfo.lastModified should equal (imgInfoCmp2.lastModified)

    // check fields
    imgInfo.fields.get("name").size should equal (imgInfoCmp.fields.get("name").size)
    imgInfo.fields.get("name").size should equal (imgInfoCmp2.fields.get("name").size)

    (imgInfoCmp: @unchecked) match {
      case FileInfoton(_,_,_,_,fields , content, _ ) =>
        content.get match {
          case FileContent(data,mimeType,_,_) =>
            val d = data.get
            d should equal (s)
            "image/jpeg;charset=iso-8859-1" should equal (mimeType)
      }
    }

    (imgInfoCmp2: @unchecked) match {
      case FileInfoton(_,_,_,_,fields , content, _ ) =>
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
    val textInfo = FileInfoton("/command-test/objinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , text )
    val textInfoCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(textInfo))
    val textInfoCmp2 = serialize2Anddeserialize2(textInfo)

    // check system
    textInfo.path should equal (textInfoCmp.path)
    textInfo.uuid should equal (textInfoCmp.uuid)
    textInfo.lastModified should equal (textInfoCmp.lastModified)
    textInfo.path should equal (textInfoCmp2.path)
    textInfo.uuid should equal (textInfoCmp2.uuid)
    textInfo.lastModified should equal (textInfoCmp2.lastModified)

    // check fields
    textInfo.fields.get("name").size should equal (textInfoCmp.fields.get("name").size)
    textInfo.fields.get("name").size should equal (textInfoCmp2.fields.get("name").size)

    (textInfoCmp: @unchecked) match {
      case FileInfoton(_,_,_,_,fields , content, _ ) =>
        content.get match {
          case FileContent(data,mimeType,_,_) =>
            val d = data.get
            d should equal (s)
            "text/plain;charset=utf-8" should equal (mimeType)
      }
    }

    (textInfoCmp2: @unchecked) match {
      case FileInfoton(_,_,_,_,fields , content, _ ) =>
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
    val fInf = FileInfoton("/command-test/fileinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))), data)
    val dataInfoCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(fInf))
    val dataInfoCmp2 = serialize2Anddeserialize2(fInf)

    // check system
    fInf.path should equal (dataInfoCmp.path)
    fInf.uuid should equal (dataInfoCmp.uuid)
    fInf.lastModified should equal (dataInfoCmp.lastModified)
    fInf.path should equal (dataInfoCmp2.path)
    fInf.uuid should equal (dataInfoCmp2.uuid)
    fInf.lastModified should equal (dataInfoCmp2.lastModified)

    // check fields
    fInf.fields.get("name").size should equal (dataInfoCmp.fields.get("name").size)
    fInf.fields.get("name").size should equal (dataInfoCmp2.fields.get("name").size)

    (dataInfoCmp: @unchecked) match {
      case FileInfoton(_,_,_,_,_,content,_) =>
        content.get match {
          case FileContent(binData,mimeType,_,_) =>
            val d = binData.get
            d should equal (bArr)
            "application/octet-stream" should equal (mimeType)
      }
    }

    (dataInfoCmp2: @unchecked) match {
      case FileInfoton(_,_,_,_,_,content,_) =>
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
    val fInf = FileInfoton("/command-test/fileinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))), data)
    val dataInfoCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(fInf))
    val dataInfoCmp2 = serialize2Anddeserialize2(fInf)

    // check system
    fInf.path should equal (dataInfoCmp.path)
    fInf.uuid should equal (dataInfoCmp.uuid)
    fInf.lastModified should equal (dataInfoCmp.lastModified)
    fInf.path should equal (dataInfoCmp2.path)
    fInf.uuid should equal (dataInfoCmp2.uuid)
    fInf.lastModified should equal (dataInfoCmp2.lastModified)

    // check fields
    fInf.fields.get("name").size should equal (dataInfoCmp.fields.get("name").size)
    fInf.fields.get("name").size should equal (dataInfoCmp2.fields.get("name").size)

    (dataInfoCmp: @unchecked) match {
      case FileInfoton(_,_,_,_,_,content,_) =>
        content.get match {
          case FileContent(binData,mimeType,_,_) =>
            val d = binData.get
            d should equal (bArr)
            "application/octet-stream" should equal (mimeType)
        }
    }

    (dataInfoCmp2: @unchecked) match {
      case FileInfoton(_,_,_,_,_,content,_) =>
        content.get match {
          case FileContent(binData,mimeType,_,_) =>
            val d = binData.get
            d should equal (bArr)
            "application/octet-stream" should equal (mimeType)
        }
    }
  }

  "link infoton serializer" should "be successful" in {
    val forward = LinkInfoton("/command-test/objinfo1","dc_test" , Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Forward )

    val forwardCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(forward))
    // check system
    forward.path should equal (forwardCmp.path)
    forward.uuid should equal (forwardCmp.uuid)
    forward.lastModified should equal (forwardCmp.lastModified)
    // check link
    (forwardCmp: @unchecked) match {
      case LinkInfoton(_,_,_,_,_,t,lt,_) => {
        forward.linkTo should equal (t)
        forward.linkType should equal (lt)
      }
    }
    // check fields
    forward.fields.get("name").size should equal (forwardCmp.fields.get("name").size)

    val per = LinkInfoton("/command-test/objinfo1","dc_test", Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Permanent )

    val perCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(per))
    // check system
    per.path should equal (perCmp.path)
    per.uuid should equal (perCmp.uuid)
    per.lastModified should equal (perCmp.lastModified)
    // check link
    (perCmp: @unchecked) match {
      case LinkInfoton(_,_,_,_,_,t,lt,_ ) => {
        per.linkTo should equal (t)
        per.linkType should equal (lt)
      }
    }
    // check fields
    per.fields.get("name").size should equal (perCmp.fields.get("name").size)


    val temp = LinkInfoton("/command-test/objinfo1","dc_test" , Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Temporary )

    val tempCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(temp))
    // check system
    temp.path should equal (tempCmp.path)
    temp.uuid should equal (tempCmp.uuid)
    temp.lastModified should equal (tempCmp.lastModified)
    // check link
    (tempCmp: @unchecked) match {
      case LinkInfoton(_,_,_,_,_,t,lt,_ ) => {
        temp.linkTo should equal (t)
        temp.linkType should equal (lt)
      }
    }
    // check fields
    temp.fields.get("name").size should equal (tempCmp.fields.get("name").size)

  }

  "deleted infoton serializer" should "be successful" in {
    val deleted = DeletedInfoton("/command-test/delete","dc_test")
    val deletedCmp = InfotonSerializer.deserialize(InfotonSerializer.serialize(deleted))

    deleted.path should equal (deletedCmp.path)
    deleted.lastModified should equal (deletedCmp.lastModified)
  }

  "diffetent infotons with same fields" should "return isSameAs==true" in {
    val infoton1 = ObjectInfoton("/pathOfInfoton1","dc_test", None, new DateTime("2015-03-04T12:51:39.000Z"), Map("Mark"->Set[FieldValue](FString("King"),FString("Awesome"))))
    val infoton2 = ObjectInfoton("/pathOfInfoton2","dc_test", None, new DateTime("2001-02-03T09:34:21.000Z"), Map("Mark"->Set[FieldValue](FString("Awesome"),FString("King"))))

    (infoton1 isSameAs infoton2) should equal (true)
  }

}
