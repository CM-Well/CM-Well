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


package cmwell.tools.data.downloader.consumer

import org.scalatest.{FlatSpec, Matchers}

class ConsumeStateHandlerSpec extends FlatSpec with Matchers{
  import ConsumeStateHandler._

  "success state" should "become success state after success event" in {
    val result = ConsumeStateHandler.nextSuccess(SuccessState(0))
    result should be (SuccessState(0))
  }

  it should "become success state after small amount of failures" in {
    val result = ConsumeStateHandler.nextFailure(SuccessState(successToLightFailureThreshold - 1))
    result should be (SuccessState(successToLightFailureThreshold))
  }

  it should "become light failure state after failures" in {
    val result = ConsumeStateHandler.nextFailure(SuccessState(successToLightFailureThreshold))
    result should be (LightFailure(0, 0))
  }

  "light failure state" should "become success state after success" in {
    val result = ConsumeStateHandler.nextSuccess(LightFailure(0, lightFailureToSuccessThreshold))
    result should be (SuccessState(0))
  }

  it should "become light failure state after success" in {
    val result = ConsumeStateHandler.nextSuccess(LightFailure(0, lightFailureToSuccessThreshold - 1))
    result should be (LightFailure(0, lightFailureToSuccessThreshold))
  }

  it should "become heavy failure state after failure" in {
    val result = ConsumeStateHandler.nextFailure(LightFailure(lightToHeavyFailureThreshold, 0))
    result should be (HeavyFailure(0))
  }

  it should "become light failure state after failure" in {
    val result = ConsumeStateHandler.nextFailure(LightFailure(0, 0))
    result should be (LightFailure(1, 0))
  }

  "heavy failure state" should "become light failure state after success" in {
    val result = ConsumeStateHandler.nextSuccess(HeavyFailure(heavyFailureToLightFailureThreshold))
    result should be (LightFailure(0, 0))
  }

  it should "become heavy failure state after success" in {
    val result = ConsumeStateHandler.nextSuccess(HeavyFailure(heavyFailureToLightFailureThreshold - 1))
    result should be (HeavyFailure(heavyFailureToLightFailureThreshold))
  }

  it should "become heavy failure state after failure" in {
    val result = ConsumeStateHandler.nextFailure(HeavyFailure(0))
    result should be (HeavyFailure(0))
  }
}
