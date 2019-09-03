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


package cmwell.bg.test

import cmwell.bg.Merger
import cmwell.domain.{FNull, FieldValue, ObjectInfoton}
import cmwell.common.{DeletePathCommand, UpdatePathCommand, WriteCommand}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers, OptionValues}

/**
  * Created by israel on 29/11/2016.
  */
class BGMergerSpec extends FlatSpec with Matchers with OptionValues {
  val merger = Merger()

  "Merger" should "merge WriteCommand with no previous version correctly" in {
    val infoton = ObjectInfoton(
      "/bg-test-merge/objinfo1",
      "dc1",
      None,
      new DateTime(),
      "Baruch",
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith"))),
      None
    )
    val writeCommand = WriteCommand(infoton)
    val merged = merger.merge(None, Seq(writeCommand)).merged
    merged.value shouldEqual infoton
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is greater" in {
    val now = DateTime.now()
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      "Baruch",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val currentDateTime = now.plus(1L)
    val current = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      currentDateTime,
      "Baruch2",
      Map("last-name" -> Set(FieldValue("smith"))),
      None
    )
    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      merger.defaultDC,
      None,
      currentDateTime,
      "Baruch2",
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith"))),
      None
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged
    merged.value shouldEqual expected
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is equal" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      "Baruch",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val current = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      "Baruch2",
      Map("last-name" -> Set(FieldValue("smith"))),
      None
    )
    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      merger.defaultDC,
      None,
      now.plus(1L),
      "Baruch2",
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith"))),
      None
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged
    merged.value shouldEqual expected
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is less than" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(1L),
      "Baruch",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val current = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      "Baruch2",
      Map("last-name" -> Set(FieldValue("smith"))),
      None
    )
    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      merger.defaultDC,
      None,
      now.plus(2L),
      "Baruch2",
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith"))),
      None
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged
    merged.value shouldEqual expected
  }

  it should "merge lastModifiedBy when 2 different users add fields at the same time" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      "Baruch",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val change1 = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(5L),
      "Baruch2",
      Map("last-name" -> Set(FieldValue("smith"))),
      None
    )
    val change2 = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(10L),
      "Baruch3",
      Map("address" -> Set(FieldValue("Petach Tikva"))),
      None
    )
    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      merger.defaultDC,
      None,
      now.plus(10L),
      "Baruch2,Baruch3",
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")),
        "address" -> Set(FieldValue("Petach Tikva"))),
      None
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(change1), WriteCommand(change2))).merged
    merged.value shouldEqual expected
  }

  it should "merge lastModifiedBy when same user add more than 1 field at the same time" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      "Baruch",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val change1 = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(5L),
      "Baruch2",
      Map("last-name" -> Set(FieldValue("smith"))),
      None
    )
    val change2 = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(10L),
      "Baruch2",
      Map("address" -> Set(FieldValue("Petach Tikva"))),
      None
    )
    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      merger.defaultDC,
      None,
      now.plus(10L),
      "Baruch2",
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")),
        "address" -> Set(FieldValue("Petach Tikva"))),
      None
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(change1), WriteCommand(change2))).merged
    merged.value shouldEqual expected
  }

  it should "Null update case should not change the user name" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(1L),
      "Baruch",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val change1 = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(5L),
      "Baruch2",
      Map("first-name" -> Set(FieldValue("john"))),
      None
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(change1))).merged
    merged should be(None)
  }

  //  it should "merge DeletePathCommand with no previous version correctly" in {
  //    val previous = ObjectInfoton()
  //    merger.merge(None, Seq(DeletePathCommand("/be-test-merge/delpathnoprev"))) shouldEqual(None)
  //  }

  it should "merge DeletePathCommand with previous version correctly" in {
    merger.merge(None, Seq(DeletePathCommand("/be-test-merge/delpathnoprev", lastModifiedBy="Baruch"))).merged shouldBe empty
  }

  it should "merge odd number of virtual parents commands with no previous version correctly" in {
    val infoton = ObjectInfoton(
      "/bg-test-merge/virtualparentodd1",
      "dc1",
      None,
      new DateTime(0L),
      "Baruch",
      None,
      "",
      None
    )
    val infotons = Seq.tabulate(7)(_ => infoton)
    val writeCommands = infotons.map(WriteCommand(_))
    val merged = merger.merge(None, writeCommands).merged
    merged.value shouldEqual infoton.copyInfoton(lastModified = new DateTime(0L), dc = merger.defaultDC)
  }

  it should "merge even number of virtual parents commands with no previous version correctly" in {
    val infoton = ObjectInfoton(
      "/bg-test-merge/virtualparenteven1",
      "dc1",
      None,
      new DateTime(0L),
      "Baruch",
      None,
      "",
      None
    )
    val infotons = Seq.tabulate(10)(_ => infoton)
    val writeCommands = infotons.map(WriteCommand(_))
    val merged = merger.merge(None, writeCommands).merged
    merged.value shouldEqual infoton.copyInfoton(lastModified = new DateTime(0L), dc = merger.defaultDC)
  }

  it should "merge odd number of virtual parents commands with a previous version correctly" in {
    val infoton = ObjectInfoton(
      s"/bg-test-merge/virtualparentodd1",
      "dc1",
      Some(1L),
      new DateTime(0L),
      "Baruch",
      None,
      "",
      None
    )
    val infotons = Seq.tabulate(7){ _ => infoton}
    val writeCommands = infotons.map{WriteCommand(_)}
    val merged = merger.merge(Some(infoton), writeCommands).merged
    merged shouldBe empty
  }

  it should "merge even number of virtual parents commands with a previous version correctly" in {
    val infoton = ObjectInfoton(
      s"/bg-test-merge/virtualparenteven1",
      "dc1",
      Some(2L),
      new DateTime(0L),
      "Baruch",
      None,
      "",
      None
    )
    val infotons = Seq.tabulate(10){ _ => infoton}
    val writeCommands = infotons.map{WriteCommand(_)}
    val merged = merger.merge(Some(infoton), writeCommands).merged
    merged shouldBe empty
  }

  it should "merge null update commands with no base correctly" in {
    val now = DateTime.now
    val infoton1 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now,
      "Baruch",
      None,
      "",
      None
    )
    val infoton2 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now.plusMillis(20),
      "Baruch",
      None,
      "",
      None
    )
    val writeCommand1 = WriteCommand(infoton1)
    val writeCommand2 = WriteCommand(infoton2)
    val merged = merger.merge(None, Seq(writeCommand1, writeCommand2))
    //Taking care of dataCenter
    merged.merged shouldEqual(Some(infoton2.copy(dc=merger.defaultDC)))
  }

  it should "merge not-indexed base infoton with identical command correctly" in {
    val baseInfoton = ObjectInfoton(
      "/bg-test-merge/infonotindexed1",
      "dc1",
      None,
      DateTime.now(),
      "Baruch",
      None,
      "",
      None
    )
    val writeCommand = WriteCommand(baseInfoton.copyInfoton(lastModified = baseInfoton.lastModified.minus(1)))
    val merged = merger.merge(Some(baseInfoton), Seq(writeCommand))
    withClue(merged){
      merged.merged shouldNot be (defined)
    }
  }

  it should "merge correctly infoton with updatePathCommand" in {
    val now = DateTime.now()
    val baseInfoton = ObjectInfoton(
      "/bg-test-merge/infonotindexed1",
      "dc",
      None,
      now,
      "Baruch",
      Some(Map("prdct.JeRn0A" -> Set(FieldValue("v3")))),
      "",
      None
    )
    val updateCommand = UpdatePathCommand(path = baseInfoton.path, deleteFields = Map("prdct.JeRn0A" -> Set(FNull(None))),
      updateFields = Map("prdct.JeRn0A" -> Set(FieldValue("v3"))), lastModified = now.plus(5L),
      lastModifiedBy = "Updater", protocol = Some("https"))

    val expected = ObjectInfoton(
      "/bg-test-merge/infonotindexed1",
      "dc",
      None,
      now.plus(5L),
      "Updater",
      Some(Map("prdct.JeRn0A" -> Set(FieldValue("v3")))),
      "",
      Some("https")
    )

    val merged = merger.merge(Some(baseInfoton), Seq(updateCommand))

    withClue(merged){
      merged.merged shouldEqual(Some(expected))
    }
  }

  it should "merge null update commands with different base correctly" in {
    val now = DateTime.now
    val baseInfoton = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      Some(1L),
      now,
      "Baruch",
      None,
      "",
      None
    )
    val infoton1 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now.minusMillis(161),
      "Baruch",
      None,
      "",
      None
    )
    val infoton2 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now.plusMillis(53),
      "Baruch",
      None,
      "",
      None
    )
    val writeCommand1 = WriteCommand(infoton1)
    val writeCommand2 = WriteCommand(infoton2)
    val merged = merger.merge(Some(baseInfoton), Seq(writeCommand2, writeCommand1))
    merged.merged shouldBe empty
  }
}
