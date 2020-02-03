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


package cmwell.bg.test

import domain.testUtil.InfotonGenerator.genericSystemFields
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
    val infoton = ObjectInfoton(genericSystemFields.copy(path = "/bg-test-merge/objinfo1", dc = "dc1"),
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )
    val writeCommand = WriteCommand(infoton)
    val merged = merger.merge(None, Seq(writeCommand)).merged
    withClue(s"infoton: $infoton\nmerged: $merged"){
      merged.value shouldEqual infoton
    }
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is greater" in {
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/objinfo2", dc = "dc1")
    val previous  = ObjectInfoton(systemFields, Map("first-name" -> Set(FieldValue("john"))))
    val currentDateTime = systemFields.lastModified.plus(1L)
    val current = ObjectInfoton(systemFields.copy(lastModified = currentDateTime, lastModifiedBy = "Baruch2"),
      Map("last-name" -> Set(FieldValue("smith")))
    )
    val expected = ObjectInfoton(systemFields.copy(dc = merger.defaultDC, lastModified = currentDateTime,
      lastModifiedBy = "Baruch2", indexTime = None), Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged
    withClue(s"previous: $previous\ncurrent: $current\nexpected: $expected\nmerged: $merged") {
      merged.value shouldEqual expected
    }
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is equal" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/objinfo2", dc = "dc1", lastModified = now)
    val previous  = ObjectInfoton(systemFields, Map("first-name" -> Set(FieldValue("john"))))
    val current = ObjectInfoton(systemFields.copy(lastModifiedBy = "Baruch2"), Map("last-name" -> Set(FieldValue("smith"))))

    val expected = ObjectInfoton(systemFields.copy(dc = merger.defaultDC, lastModified = now.plus(1L),
      lastModifiedBy = "Baruch2", indexTime = None),
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged
    withClue(s"previous: $previous\ncurrent: $current\nexpected: $expected\nmerged: $merged") {
      merged.value shouldEqual expected
    }
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is less than" in {
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/objinfo2", dc = "dc1")
    val previous  = ObjectInfoton(systemFields.copy(lastModified = systemFields.lastModified.plus(1L)),
      Map("first-name" -> Set(FieldValue("john")))
    )
    val current = ObjectInfoton(systemFields.copy(lastModifiedBy = "Baruch2"),
      Map("last-name" -> Set(FieldValue("smith")))
    )
    val expected = ObjectInfoton(systemFields.copy(dc = merger.defaultDC, lastModified = systemFields.lastModified.plus(2L),
      lastModifiedBy = "Baruch2", indexTime = None), Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged
    withClue(s"previous: $previous\ncurrent: $current\nexpected: $expected\nmerged: $merged") {
      merged.value shouldEqual expected
    }
  }

  it should "merge lastModifiedBy when 2 different users add fields at the same time" in {
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/objinfo2", dc = "dc1")
    val previous  = ObjectInfoton(systemFields, Map("first-name" -> Set(FieldValue("john")))
    )
    val change1 = ObjectInfoton(systemFields.copy(lastModified = systemFields.lastModified.plus(5L),
      lastModifiedBy = "Baruch2"), Map("last-name" -> Set(FieldValue("smith")))
    )
    val change2 = ObjectInfoton(systemFields.copy(lastModified = systemFields.lastModified.plus(10L), lastModifiedBy = "Baruch3"),
      Map("address" -> Set(FieldValue("Petach Tikva")))
    )
    val expected = ObjectInfoton(systemFields.copy(dc = merger.defaultDC, lastModified = systemFields.lastModified.plus(10L),
      lastModifiedBy = "Baruch2,Baruch3", indexTime = None), Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")),
      "address" -> Set(FieldValue("Petach Tikva")))
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(change1), WriteCommand(change2))).merged
    withClue(s"previous: $previous\nchange1: $change1\nchange2: $change2\nexpected: $expected\nmerged: $merged") {
      merged.value shouldEqual expected
    }
  }

  it should "merge lastModifiedBy when same user add more than 1 field at the same time" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/objinfo2", dc = "dc1")
    val previous  = ObjectInfoton(systemFields, Map("first-name" -> Set(FieldValue("john"))))
    val change1 = ObjectInfoton(systemFields.copy(lastModified = now.plus(5L), lastModifiedBy = "Baruch2"),
      Map("last-name" -> Set(FieldValue("smith")))
    )
    val change2 = ObjectInfoton(systemFields.copy(lastModified = now.plus(10L), lastModifiedBy = "Baruch2"),
      Map("address" -> Set(FieldValue("Petach Tikva")))
    )
    val expected = ObjectInfoton(systemFields.copy(dc = merger.defaultDC, lastModified = now.plus(10L),
      lastModifiedBy = "Baruch2", indexTime = None), Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")),
        "address" -> Set(FieldValue("Petach Tikva")))
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(change1), WriteCommand(change2))).merged
    withClue(s"previous: $previous\nchange1: $change1\nchange2:$change2\nexpected: $expected\nmerged: $merged") {
      merged.value shouldEqual expected
    }
  }

  it should "Null update case should not change the user name" in {
    val now = DateTime.now(DateTimeZone.UTC)
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/objinfo2", dc = "dc1")
    val previous  = ObjectInfoton(systemFields.copy(lastModified = now.plus(1L)),
      Map("first-name" -> Set(FieldValue("john")))
    )
    val change1 = ObjectInfoton(systemFields.copy(lastModified = now.plus(5L), lastModifiedBy = "Baruch2"),
      Map("first-name" -> Set(FieldValue("john")))
    )
    val merged = merger.merge(Some(previous), Seq(WriteCommand(change1))).merged
    withClue(s"previous: $previous\nchange1: $change1\nmerged: $merged") {
      merged should be(None)
    }
  }

  //  it should "merge DeletePathCommand with no previous version correctly" in {
  //    val previous = ObjectInfoton()
  //    merger.merge(None, Seq(DeletePathCommand("/be-test-merge/delpathnoprev"))) shouldEqual(None)
  //  }

  it should "merge DeletePathCommand with previous version correctly" in {
    merger.merge(None, Seq(DeletePathCommand("/be-test-merge/delpathnoprev", lastModifiedBy="Baruch"))).merged shouldBe empty
  }

  it should "merge odd number of virtual parents commands with no previous version correctly" in {
    val infoton = ObjectInfoton(genericSystemFields.copy(path = "/bg-test-merge/virtualparentodd1", dc = "dc1",
      lastModified = new DateTime(0L), indexTime = None))
    val infotons = Seq.tabulate(7)(_ => infoton)
    val writeCommands = infotons.map(WriteCommand(_))
    val merged = merger.merge(None, writeCommands).merged
    withClue(s"infoton: $infoton\nmerged: $merged") {
      merged.value shouldEqual infoton.copyInfoton(infoton.systemFields.copy(lastModified = new DateTime(0L), dc = merger.defaultDC))
    }
  }

  it should "merge even number of virtual parents commands with no previous version correctly" in {
    val infoton = ObjectInfoton(genericSystemFields.copy(path = "/bg-test-merge/virtualparenteven1", dc = "dc1", lastModified = new DateTime(0L)))
    val infotons = Seq.tabulate(10)(_ => infoton)
    val writeCommands = infotons.map(WriteCommand(_))
    val merged = merger.merge(None, writeCommands).merged
    withClue(s"infoton: $infoton\nmerged: $merged") {
      merged.value shouldEqual infoton.copyInfoton(infoton.systemFields.copy(
        lastModified = new DateTime(0L), dc = merger.defaultDC, indexTime = None))
    }
  }

  it should "merge odd number of virtual parents commands with a previous version correctly" in {
    val infoton = ObjectInfoton(genericSystemFields.copy(path = "/bg-test-merge/virtualparenteven1", dc = "dc1", indexTime = Some(1L)))
    val infotons = Seq.tabulate(7){ _ => infoton}
    val writeCommands = infotons.map{WriteCommand(_)}
    val merged = merger.merge(Some(infoton), writeCommands).merged
    withClue(s"infoton: $infoton\nmerged: $merged") {
      merged shouldBe empty
    }
  }

  it should "merge even number of virtual parents commands with a previous version correctly" in {
    val infoton = ObjectInfoton(genericSystemFields.copy(path = "/bg-test-merge/virtualparenteven1", dc = "dc1", indexTime = Some(2L)))
    val infotons = Seq.tabulate(10){ _ => infoton}
    val writeCommands = infotons.map{WriteCommand(_)}
    val merged = merger.merge(Some(infoton), writeCommands).merged
    withClue(s"infoton: $infoton\nmerged: $merged") {
      merged shouldBe empty
    }
  }

  it should "merge null update commands with no base correctly" in {
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/infonull1", dc = "dc1")
    val infoton1 = ObjectInfoton(systemFields)
    val infoton2 = ObjectInfoton(systemFields.copy(lastModified = systemFields.lastModified.plusMillis(20)))
    val writeCommand1 = WriteCommand(infoton1)
    val writeCommand2 = WriteCommand(infoton2)
    val merged = merger.merge(None, Seq(writeCommand1, writeCommand2))
    //Taking care of dataCenter
    withClue(s"infoton1: $infoton1\ninfoton2: $infoton2\nmerged: $merged") {
      merged.merged shouldEqual (Some(infoton2.copy(infoton2.systemFields.copy(dc = merger.defaultDC, indexTime = None))))
    }
  }

  it should "merge not-indexed base infoton with identical command correctly" in {
    val baseInfoton = ObjectInfoton(genericSystemFields.copy(path = "/bg-test-merge/infonotindexed1", dc = "dc1"))
    val writeCommand = WriteCommand(baseInfoton.copyInfoton(baseInfoton.systemFields.copy(lastModified = baseInfoton.systemFields.lastModified.minus(1))))
    val merged = merger.merge(Some(baseInfoton), Seq(writeCommand))
    withClue(s"baseInfoton: $baseInfoton merged: $merged"){
      merged.merged shouldNot be (defined)
    }
  }

  it should "merge correctly infoton with updatePathCommand" in {
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/infonotindexed1", dc = "dc", protocol = "http")
    val baseInfoton = ObjectInfoton(systemFields, Some(Map("prdct.JeRn0A" -> Set(FieldValue("v3")))))
    val updateCommand = UpdatePathCommand(path = systemFields.path, deleteFields = Map("prdct.JeRn0A" -> Set(FNull(None))),
      updateFields = Map("prdct.JeRn0A" -> Set(FieldValue("v3"))), lastModified = systemFields.lastModified.plus(5L),
      lastModifiedBy = "Updater", protocol = "https")

    val expected = ObjectInfoton(systemFields.copy(lastModified = systemFields.lastModified.plus(5L),
      lastModifiedBy = "Updater", protocol = "https", indexTime = None), Some(Map("prdct.JeRn0A" -> Set(FieldValue("v3")))))

    val merged = merger.merge(Some(baseInfoton), Seq(updateCommand))

    withClue(s"baseInfoton: $baseInfoton\nupdateCommand: $updateCommand\nexpected: $expected\nmerged: $merged") {
      merged.merged shouldEqual(Some(expected))
    }
  }

  it should "merge null update commands with different base correctly" in {
    val now = DateTime.now
    val systemFields = genericSystemFields.copy(path = "/bg-test-merge/infonull1", dc = "dc1", lastModified = now)
    val baseInfoton = ObjectInfoton(systemFields.copy(indexTime = Some(1L)))
    val infoton1 = ObjectInfoton(systemFields.copy(lastModified = now.minusMillis(161)))
    val infoton2 = ObjectInfoton(systemFields.copy(lastModified = now.plusMillis(53)))
    val writeCommand1 = WriteCommand(infoton1)
    val writeCommand2 = WriteCommand(infoton2)
    val merged = merger.merge(Some(baseInfoton), Seq(writeCommand2, writeCommand1))
    withClue(s"baseInfoton: $baseInfoton\ninfoton1: $infoton1\ninfoton2: $infoton2\nmerged: $merged") {
      merged.merged shouldBe empty
    }
  }
}
