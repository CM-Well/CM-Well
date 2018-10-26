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
import cmwell.rts.Rule
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by markz on 1/19/15.
 */
class RuleSpec extends FlatSpec with Matchers {
  "path rule" should "be successful" in {
    // only exact match + 1 level
    val r1 = Rule("/mark/test",false)
    val r2 = Rule("/mark/test",true)

    false should equal (r1.path check ObjectInfoton("/mark","dc_test",protocol=None).path)
    true should equal  (r1.path check ObjectInfoton("/mark/test","dc_test",protocol=None).path)
    true should equal  (r1.path check ObjectInfoton("/mark/test/1","dc_test",protocol=None).path)
    false should equal (r1.path check ObjectInfoton("/mark/test/1/2/","dc_test",protocol=None).path)

    false should equal(r2.path check ObjectInfoton("/mark","dc_test",protocol=None).path)
    true should equal (r2.path check  ObjectInfoton("/mark/test","dc_test",protocol=None).path)
    true should equal (r2.path check  ObjectInfoton("/mark/test/1","dc_test",protocol=None).path)
    true should equal(r2.path check ObjectInfoton("/mark/test/1/2/","dc_test",protocol=None).path)
  }


  "match rule" should "be successful" in {
    val m1 = Rule(Map.empty[String,Set[FieldValue]])

    val m2 = Rule(Map[String,Set[FieldValue]]("kids" -> Set(FString("gal") , FString("yoav") , FString ("roni")),
                      "name" -> Set(),
                      "age" -> Set(FInt(34)) ))
    // because empty map always return true
    true should equal ( m1.matchMap check Map("kids" -> Set(FString("gal"))) )
    // chekc if have a key - kids and value gal
    true should equal ( m2.matchMap check Map("kids" -> Set(FString("gal")) ) ) // true
    false should equal ( m2.matchMap check Map("kids" -> Set(FString("gilad")) )) // false

    true should equal ( m2.matchMap check Map("name" -> Set(FString("mark")) )) // true check is the key exists
    false should equal ( m2.matchMap check Map("last_name" -> Set(FString("z")) )) // false check is the key exists


    true should equal ( m2.matchMap check Map("age" -> Set(FInt(34)))) // true checks int
    false should equal ( m2.matchMap check Map("age" -> Set(FInt(30)))) // false checks int

  }
}
