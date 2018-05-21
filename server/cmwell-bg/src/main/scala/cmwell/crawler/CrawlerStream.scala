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

package cmwell.crawler

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.concurrent.{ExecutionContext, Future}

case class CrawlerPosition(offset: Long, timeStamp: Long)
sealed trait CrawlerState
case class CrawlerBounds(startingOffset: Long, endOffset: Long) extends CrawlerState
case object NullCrawlerBounds extends CrawlerState

object CrawlerStream extends LazyLogging {

  /*
  def createCrawlerStream(startingPoint: CrawlerPosition)(sys: ActorSystem) = {
    positionSource(startingPoint)
      .flatMapConcat {

      }

  }
*/


  def positionSource(startingState: CrawlerPosition)(sys: ActorSystem, ec: ExecutionContext): Source[CrawlerState, NotUsed] = {
    Source.unfoldAsync(startingState) { state =>
      val zStorePosition: CrawlerPosition = ???
      if (zStorePosition.offset < state.offset) {
        val e = new Exception(s"Persisted offset [${zStorePosition.offset}] is smaller than current crawler offset [${state.offset}]. " +
          s"This should never happen. Closing crawler stream!")
        logger.error("This is no need to print an exception twice - just read it from the oncomplete log print!!!") //todo: implement in onComplete
        Future.failed[Option[(CrawlerPosition, CrawlerState)]](e)
      }
      else if (zStorePosition.offset == state.offset) {
        //Bg didn't do anything from the previous check - sleep and then emit some sentinel for another element
        //todo: delay should be configurable
        akka.pattern.after(30.seconds, sys.scheduler)(Future.successful(Some(state -> NullCrawlerBounds)))(ec)
      }
      else {
        val safetyNetTimeInMillis: Long = ??? // todo: get from config
        val now = System.currentTimeMillis()
        val timeDiff = now - zStorePosition.timeStamp
        val delayDuration = Math.max(0, safetyNetTimeInMillis - timeDiff).millis
        //todo: watch off by one errors!!
        val bounds = CrawlerBounds(state.offset + 1, zStorePosition.offset)
        akka.pattern.after(delayDuration, sys.scheduler)(Future.successful(Some(zStorePosition -> bounds)))(ec)
      }
    }
      .filter {
        case _: CrawlerBounds => true
        case NullCrawlerBounds => false
      }
  }

  def messageSource()(sys: ActorSystem) = {
    val consumerSettings = ConsumerSettings(sys, new ByteArrayDeserializer, new ByteArrayDeserializer)
/*
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
*/
    val partition = 0
    val subscription = Subscriptions.assignmentWithOffset(
      new TopicPartition("topic1", partition) -> 0L //fromOffset
    )
    Consumer.plainSource(consumerSettings, subscription)
  }
}
