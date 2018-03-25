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
import cmwell.domain.{FieldValue, ObjectInfoton}
import cmwell.common.{DeletePathCommand, WriteCommand}
import org.joda.time.DateTime
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers, OptionValues}

/**
  * Created by israel on 29/11/2016.
  */
@DoNotDiscover
class BGMergerSpec extends FlatSpec with Matchers with OptionValues {


  val merger = Merger()

  "Merger" should "merge WriteCommand with no previous version correctly" in {
    val infoton = ObjectInfoton(
      "/bg-test-merge/objinfo1",
      "dc1",
      None,
      new DateTime(),
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
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
      Map("first-name" -> Set(FieldValue("john")))
    )

    val currentDateTime = now.plus(1L)
    val current = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      currentDateTime,
      Map("last-name" -> Set(FieldValue("smith")))
    )

    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      currentDateTime,
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )

    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged

    merged.value shouldEqual expected
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is equal" in {
    val now = DateTime.now()
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      Map("first-name" -> Set(FieldValue("john")))
    )

    val current = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      Map("last-name" -> Set(FieldValue("smith")))
    )

    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(1L),
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )

    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged

    merged.value shouldEqual expected
  }

  it should "merge WriteCommand with previous version correctly when new lastModified is less than" in {
    val now = DateTime.now()
    val previous  = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(1L),
      Map("first-name" -> Set(FieldValue("john")))
    )

    val current = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now,
      Map("last-name" -> Set(FieldValue("smith")))
    )

    val expected = ObjectInfoton(
      "/bg-test-merge/objinfo2",
      "dc1",
      None,
      now.plus(2L),
      Map("first-name" -> Set(FieldValue("john")), "last-name" -> Set(FieldValue("smith")))
    )

    val merged = merger.merge(Some(previous), Seq(WriteCommand(current))).merged

    merged.value shouldEqual expected
  }

//  it should "merge DeletePathCommand with no previous version correctly" in {
//    val previous = ObjectInfoton()
//    merger.merge(None, Seq(DeletePathCommand("/be-test-merge/delpathnoprev"))) shouldEqual(None)
//  }

  it should "merge DeletePathCommand with previous version correctly" in {
    merger.merge(None, Seq(DeletePathCommand("/be-test-merge/delpathnoprev"))).merged shouldBe empty
  }

  it should "merge odd number of virtual parents commands with no previous version correctly" in {

    val infoton = ObjectInfoton(
      "/bg-test-merge/virtualparentodd1",
      "dc1",
      None,
      new DateTime(0L),
      None
    )

    val infotons = Seq.tabulate(7)(_ => infoton)

    val writeCommands = infotons.map(WriteCommand(_))

    val merged = merger.merge(None, writeCommands).merged

    merged.value shouldEqual infoton.copyInfoton(lastModified = new DateTime(0L))
  }

  it should "merge even number of virtual parents commands with no previous version correctly" in {

    val infoton = ObjectInfoton(
      "/bg-test-merge/virtualparenteven1",
      "dc1",
      None,
      new DateTime(0L),
      None
    )

    val infotons = Seq.tabulate(10)(_ => infoton)

    val writeCommands = infotons.map(WriteCommand(_))

    val merged = merger.merge(None, writeCommands).merged

    merged.value shouldEqual infoton.copyInfoton(lastModified = new DateTime(0L))
  }

  it should "merge odd number of virtual parents commands with a previous version correctly" in {

    val infoton = ObjectInfoton(
      s"/bg-test-merge/virtualparentodd1",
      "dc1",
      Some(1L),
      new DateTime(0L),
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
      None
    )
    val infoton2 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now.plusMillis(20),
      None
    )
    val writeCommand1 = WriteCommand(infoton1)
    val writeCommand2 = WriteCommand(infoton2)
    val merged = merger.merge(None, Seq(writeCommand1, writeCommand2))

    merged.merged shouldEqual(Some(infoton2))

  }

  it should "merge not-indexed base infoton with identical command correctly" in {
    val baseInfoton = ObjectInfoton(
      "/bg-test-merge/infonotindexed1",
      "dc1",
      None,
      DateTime.now(),
      None
    )
    val writeCommand = WriteCommand(baseInfoton.copyInfoton(lastModified = baseInfoton.lastModified.minus(1)))
    val merged = merger.merge(Some(baseInfoton), Seq(writeCommand))
    withClue(merged){
      merged.merged shouldBe defined
    }
  }

  it should "merge null update commands with different base correctly" in {
    val now = DateTime.now
    val baseInfoton = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      Some(1L),
      now,
      None
    )
    val infoton1 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now.minusMillis(161),
      None
    )
    val infoton2 = ObjectInfoton(
      "/bg-test-merge/infonull1",
      "dc1",
      None,
      now.plusMillis(53),
      None
    )
    val writeCommand1 = WriteCommand(infoton1)
    val writeCommand2 = WriteCommand(infoton2)
    val merged = merger.merge(Some(baseInfoton), Seq(writeCommand2, writeCommand1))

    merged.merged shouldBe empty

  }

}
