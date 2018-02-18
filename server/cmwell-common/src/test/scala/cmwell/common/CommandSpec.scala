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


package cmwell.common

import cmwell.domain.{FString, _}
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest._

/**
* Created with IntelliJ IDEA.
* User: markz
* Date: 12/18/12
* Time: 9:04 AM
*
*/

class CommandSpec extends FlatSpec with Matchers {


  "command encode decode" should "be successful" in {
    System.setProperty("dataCenter.id" , "dc_test")
    //Log.TRACE() turns on logging of chill
    // create object infoton
    val objInfo = ObjectInfoton("/command-test/objinfo1","dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))))
    // create all commands case classes for serialization testing
    val linkInfo01 = LinkInfoton("/command-test/objinfo1","dc_test", Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Forward )
    val linkInfo02 = LinkInfoton("/command-test/objinfo1","dc_test", Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Permanent )
    val linkInfo03 = LinkInfoton("/command-test/objinfo1","dc_test", Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))) , "/mark" , LinkType.Temporary )

    val cmdWrite = WriteCommand(objInfo)
    val cmdDeletePath = DeletePathCommand("/command-test/deletePath",new DateTime)
    val cmdDeletePathAttributeValues = DeleteAttributesCommand("/command-test/deletePath",Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))),new DateTime)
    // TODO : need to understand way the next test are having problem when adding the line to the test.
    //val cmdMerged1 = MergedInfotonCommand( None , "/command-test/mergedPath")
    val t = ("/command-test/mergedPath1",0L)
    val cmdMerged2 = MergedInfotonCommand( Some( t ) , ("/command-test/mergedPath2",0) )
    val bulkCmd = List(cmdWrite,cmdWrite)

    val cmdBulkCommand = BulkCommand(bulkCmd)

    val updateDate = new DateTime

    val deleteF = Map("location" -> Set[FieldValue](FString("usa")) , "company" -> Set[FieldValue](FString("IBM") , FString("FACEBOOK")))

    val updateF = Map("location" -> Set[FieldValue](FString("israel")) , "company" -> Set[FieldValue](FString("GOOGLE")) )

    val cmdUpdate = UpdatePathCommand("/command-test/update" , deleteF , updateF , updateDate)

    val cmds = Vector(cmdWrite , cmdDeletePathAttributeValues , cmdBulkCommand , cmdDeletePath   /*, cmdMerged1*/ , cmdMerged2 , cmdUpdate)

    val c = Vector( WriteCommand(linkInfo01) , WriteCommand(linkInfo02) , WriteCommand(linkInfo03) )


    // check encode decode of LinkInfoton
    for ( item <- c ) {
      val payload : Array[Byte] = CommandSerializer.encode(item)
      val cmpCommand = CommandSerializer.decode(payload)
      cmpCommand match {
        case WriteCommand(i: LinkInfoton, trackingID, prevUUID) =>
          i.path should equal (item.infoton.path)
        case _ =>
      }

    }


    for ( item <- cmds ) {
      val payload : Array[Byte] = CommandSerializer.encode(item)
      val cmpCommand = CommandSerializer.decode(payload)
      //println(cmpCommand)

      cmpCommand match {
        case WriteCommand(infoton, trackingID, prevUUID) =>  infoton.path should equal (objInfo.path);infoton.fields.get("name").size should equal (objInfo.fields.get("name").size); infoton.lastModified.isEqual(objInfo.lastModified) should equal (true)
        case DeleteAttributesCommand(path, fields, lastModified, trackingID, prevUUID) =>  path should equal (cmdDeletePathAttributeValues.path)
        case BulkCommand(commands) =>  commands.size should equal (bulkCmd.size)
        case DeletePathCommand(path, lastModified, trackingID, prevUUID) => path should equal (cmdDeletePath.path)

        case MergedInfotonCommand(Some(prev),current,_) =>
          prev._1 should equal ("/command-test/mergedPath1")
          current._1 should equal ("/command-test/mergedPath2")

        case MergedInfotonCommand(None,current,_) =>
          current should equal ("/command-test/mergedPath")

        case UpdatePathCommand(path, d_f, u_f, lm, trackingID, prevUUID) =>
          path should equal (cmdUpdate.path)
          d_f.size should equal (cmdUpdate.deleteFields.size )
          u_f.size should equal (cmdUpdate.updateFields.size )

          lm.getMillis should equal (cmdUpdate.lastModified.getMillis)
        case OverwriteCommand(_, trackingID) => ??? //TODO: add tests for OverwriteCommand
        case OverwrittenInfotonsCommand(_, _, _) => ??? //TODO: add tests for OverwrittenInfotonsCommand
      }
    }
  }

  "file infoton decode" should "be successful" in {
    System.setProperty("dataCenter.id" , "dc_test")
    val fInfoton = FileInfoton(path="/stam/kacha",dc="dc_test", content=Some(FileContent("test text".getBytes("UTF-8"), "text/plain")))
    val payload : Array[Byte] = CommandSerializer.encode(WriteCommand(fInfoton))
    val cmpCommand = CommandSerializer.decode(payload)
    val wc = cmpCommand.asInstanceOf[WriteCommand]
    wc.infoton.asInstanceOf[FileInfoton].content.get.mimeType should equal ("text/plain")

  }

  "OverWrite encode and decode" should "be successful" in {
    val owcmd = OverwriteCommand(ObjectInfoton("/exmaple.org/spiderman","other-dc", Some(12345L), new DateTime("2015-02-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("green-goblin")))))
    val payload : Array[Byte] = CommandSerializer.encode(owcmd)
    val cmpCommand = CommandSerializer.decode(payload)
    val owc = cmpCommand.asInstanceOf[OverwriteCommand]
    owc.infoton.asInstanceOf[ObjectInfoton].indexTime should equal (Some(12345L))
  }

  "OverwrittenInfotons encode and decode 1" should "be successful" in {
    val i = ObjectInfoton("/exmaple.org/spiderman","other-dc", Some(27L),new DateTime("2015-02-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("green-goblin"))))
    val d = DeletedInfoton("/exmaple.org/spiderman","other-dc",Some(28L),new DateTime("2015-03-25T16:03:57.216Z",DateTimeZone.UTC))
    val vec = Vector((i.uuid,i.weight,i.indexTime.get),(d.uuid,d.weight,d.indexTime.get))
    val owicmd = OverwrittenInfotonsCommand(None,None,vec)
    val payload : Array[Byte] = CommandSerializer.encode(owicmd)
    val cmpCommand = CommandSerializer.decode(payload)
    val oiwc = cmpCommand.asInstanceOf[OverwrittenInfotonsCommand]
    oiwc.previousInfoton should equal (None)
    oiwc.currentInfoton should equal (None)
    oiwc.historicInfotons should equal (vec)
  }

  "OverwrittenInfotons encode and decode 2" should "be successful" in {
    val i = ObjectInfoton("/exmaple.org/spiderman","other-dc",Some(29L),new DateTime("2015-02-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("green-goblin"))))
    val j = ObjectInfoton("/exmaple.org/spiderman","other-dc",Some(30L),new DateTime("2015-03-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("dr-octopus"))))
    val vec = Vector((i.uuid,i.weight,i.indexTime.get))
    val opt = Some((j.uuid,j.weight,j.indexTime.get))
    val owicmd = OverwrittenInfotonsCommand(None,opt,vec)
    val payload : Array[Byte] = CommandSerializer.encode(owicmd)
    val cmpCommand = CommandSerializer.decode(payload)
    val oiwc = cmpCommand.asInstanceOf[OverwrittenInfotonsCommand]
    oiwc.previousInfoton should equal (None)
    oiwc.currentInfoton should equal (opt)
    oiwc.historicInfotons should equal (vec)
  }

  "OverwrittenInfotons encode and decode 3" should "be successful" in {
    val i = ObjectInfoton("/exmaple.org/spiderman","other-dc",Some(31L),new DateTime("2015-02-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("green-goblin"))))
    val j = ObjectInfoton("/exmaple.org/spiderman","other-dc",Some(32L),new DateTime("2015-03-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("dr-octopus"))))
    val optO = Some((i.uuid,i.weight))
    val optN = Some((j.uuid,j.weight,j.indexTime.get))
    val owicmd = OverwrittenInfotonsCommand(optO,optN,Vector.empty)
    val payload : Array[Byte] = CommandSerializer.encode(owicmd)
    val cmpCommand = CommandSerializer.decode(payload)
    val oiwc = cmpCommand.asInstanceOf[OverwrittenInfotonsCommand]
    oiwc.previousInfoton should equal (optO)
    oiwc.currentInfoton should equal (optN)
    oiwc.historicInfotons should equal (Vector.empty)
  }

  "OverwrittenInfotons encode and decode 4" should "be successful" in {
    val i = ObjectInfoton("/exmaple.org/spiderman","other-dc",Some(33L),new DateTime("2015-02-25T16:03:57.216Z",DateTimeZone.UTC), Map("enemyOf.rel"->Set[FieldValue](FString("green-goblin"))))
    val j = DeletedInfoton("/exmaple.org/spiderman","other-dc",Some(34L),new DateTime("2015-03-25T16:03:57.216Z",DateTimeZone.UTC))
    val optO = Some((i.uuid,i.weight))
    val vec = Vector((j.uuid,j.weight,j.indexTime.get))
    val owicmd = OverwrittenInfotonsCommand(optO,None,vec)
    val payload : Array[Byte] = CommandSerializer.encode(owicmd)
    val cmpCommand = CommandSerializer.decode(payload)
    val oiwc = cmpCommand.asInstanceOf[OverwrittenInfotonsCommand]
    oiwc.previousInfoton should equal (optO)
    oiwc.currentInfoton should equal (None)
    oiwc.historicInfotons should equal (vec)
  }

  "CommandRef" should "be successfully encoded/decoded" in {
    val commandRef = CommandRef("someuuid")
    CommandSerializer.decode(CommandSerializer.encode(commandRef)) should equal(commandRef)
  }

  "IndexNewInfotonCommand with infoton" should "be successfully encoded/decoded" in {
    val infoton = ObjectInfoton("/cmt/cm/news/1", "dc", None, DateTime.now(DateTimeZone.UTC), Map("a" -> Set[FieldValue](FString("b"))))
    val indexCommand = IndexNewInfotonCommand(infoton.uuid, true, infoton.path, Some(infoton), "")
    val payload = CommandSerializer.encode(indexCommand)
    val decodedCommand = CommandSerializer.decode(payload).asInstanceOf[IndexNewInfotonCommand]
    decodedCommand should equal (indexCommand)
  }

  "IndexNewInfotonCommand without infoton" should "be successfully encoded/decoded" in {
    val infoton = ObjectInfoton("/cmt/cm/news/1", "dc", None, DateTime.now(DateTimeZone.UTC), Map("a" -> Set[FieldValue](FString("b"))))
    val indexCommand = IndexNewInfotonCommand(infoton.uuid, true, infoton.path, None, "")
    val payload = CommandSerializer.encode(indexCommand)
    val decodedCommand = CommandSerializer.decode(payload).asInstanceOf[IndexNewInfotonCommand]
    decodedCommand should equal (indexCommand)
  }

  "TrackingID de/serialization" should "be enabled for any SingleCommand" in {
    val objInfot = ObjectInfoton("/command-test/objinfo1","dc_test", None, DateTime.now(DateTimeZone.UTC), Map("name" -> Set[FieldValue](FString("Neta-li"), FString("Shalev"))))
    val wCommand = WriteCommand(objInfot,Some("sweet_kids"))
    val eCommand = CommandSerializer.encode(wCommand)
    val dCommand = CommandSerializer.decode(eCommand).asInstanceOf[WriteCommand]
    dCommand should equal(wCommand)
  }

  "TrackingID de/serialization" should "be enabled for any IndexCommand" in {
    val objInfot = ObjectInfoton("/command-test/objinfo1","dc_test", None, DateTime.now(DateTimeZone.UTC), Map("name" -> Set[FieldValue](FString("Neta-li"), FString("Shalev"))))
    val iCommand = IndexNewInfotonCommand(objInfot.uuid,true,objInfot.path,None, "", Seq(StatusTracking("sweet",2),StatusTracking("kids",1)))
    val eCommand = CommandSerializer.encode(iCommand)
    val dCommand = CommandSerializer.decode(eCommand).asInstanceOf[IndexNewInfotonCommand]
    dCommand should equal(iCommand)
  }

  "prevUUID de/serialization" should "be enabled for any SingleCommand" in {
    val objInfot = ObjectInfoton("/command-test/objinfo1","dc_test", None, DateTime.now(DateTimeZone.UTC), Map("name" -> Set[FieldValue](FString("Neta-li"), FString("Shalev"))))
    val wCommand = WriteCommand(objInfot,None,Some("0123456789abcdef0123456789abcdef"))
    val eCommand = CommandSerializer.encode(wCommand)
    val dCommand = CommandSerializer.decode(eCommand).asInstanceOf[WriteCommand]
    dCommand should equal(wCommand)
  }

  "TrackingID and prevUUID de/serialization" should "be enabled for any SingleCommand" in {
    val objInfot = ObjectInfoton("/command-test/objinfo1","dc_test", None, DateTime.now(DateTimeZone.UTC), Map("name" -> Set[FieldValue](FString("Neta-li"), FString("Shalev"))))
    val wCommand = WriteCommand(objInfot,Some("cute_kids"),Some("0123456789abcdef0123456789abcdef"))
    val eCommand = CommandSerializer.encode(wCommand)
    val dCommand = CommandSerializer.decode(eCommand).asInstanceOf[WriteCommand]
    dCommand should equal(wCommand)
  }
}
