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


package cmwell.dc.stream

import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Supervision._
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream._
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString
import cmwell.dc.LazyLogging
import cmwell.dc.Settings._
import cmwell.dc.stream.DataCenterSyncManager.SyncerMaterialization
import cmwell.dc.stream.MessagesTypesAndExceptions.DcInfo

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

// todo: is there a better way?
import ExecutionContext.Implicits.global

/**
  * Created by eli on 16/06/16.
  */

object TsvRetrieverOldCustom {
  def apply(dataCenterId: String, decider: Decider)(implicit mat: Materializer, system: ActorSystem) = new TsvRetrieverOldCustom(dataCenterId, decider)
}

class TsvRetrieverOldCustom(dataCenterId: String, decider: Decider)(implicit val mat: Materializer, val system: ActorSystem) extends GraphStageWithMaterializedValue[SourceShape[Source[ByteString, NotUsed]], SyncerMaterialization] with LazyLogging {

  val out = Outlet[Source[ByteString, NotUsed]]("TsvRetrieverOldCustom.out")
  val shape = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, SyncerMaterialization) = {
    //Materialization data
    val startStreamPromise = Promise[DcInfo]()
    val syncDonePositionKeyPromise = Promise[String]()
    val (cancelStream, cancelFuture) = {
      val cancelPromise = Promise[Done]()
      new Cancellable {
        override def isCancelled: Boolean = cancelPromise.isCompleted

        override def cancel(): Boolean = cancelPromise.trySuccess(Done)
      } -> cancelPromise.future.map[Option[(String, Source[ByteString, NotUsed])]](_ => None)
    }

    val logic = new GraphStageLogic(shape) {
      var tsvSourceAndPositionKeyCallback: Try[(Source[ByteString, NotUsed], String)] => Unit = _
      //denotes that the stage is ready and already processed the initial position key
      var stageIsReady: Boolean = false
      var currentPositionKey: String = _
      var remoteHost: String = _

      override def preStart() = {
        tsvSourceAndPositionKeyCallback = getAsyncCallback[Try[(Source[ByteString, NotUsed], String)]] {
          case Failure(ex) => ??? //todo: this is after the retries, need to think what to do with it
          case Success((source, position)) => {
            currentPositionKey = position
            push(out, source)
          }
        }.invoke

        val startSyncHandler = getAsyncCallback { dcInfo: Try[DcInfo] =>
          currentPositionKey = dcInfo.get.positionKey.get
          remoteHost = dcInfo.get.location
          stageIsReady = true
          if (isAvailable(out)) retrieveTsvAndPush()
        }
        startStreamPromise.future.onComplete(startSyncHandler.invoke)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          //todo: how to get the host???
          if (stageIsReady) {
            setHandler(out, new OutHandler {
              override def onPull(): Unit = retrieveTsvAndPush()
            })
            retrieveTsvAndPush()
          }
        }
      })

      @inline
      def retrieveTsvAndPush(): Unit = {
        //todo: is this the right place to cancel?
        if (cancelStream.isCancelled) {
          completeStage()
          syncDonePositionKeyPromise.success(currentPositionKey)
        }
        else {
          getTsvSource(dataCenterId, currentPositionKey, remoteHost, decider).onComplete(tsvSourceAndPositionKeyCallback)
        }
      }
    }

//    val materializedValue = DataCenterSyncerStreamMaterialization(startStreamPromise, cancelStream, syncDonePositionKeyPromise.future)
//    (logic, materializedValue)
    ???
  }

  def getTsvSource(dataCenterId: String, positionKey: String, host: String, decider: Decider): Future[(Source[ByteString, NotUsed], String)] = {
    //todo: add retry here
    //todo: should I handle 204 no content specially?
    val request = HttpRequest(uri = s"http://$host/?op=consume&length-hint=$maxInfotonsPerBufferingSync&format=tsv&position=$positionKey")
    val flow =
      Http().superPool[ReqType]().map {
        case (Success(res@HttpResponse(s, h, entity, _)), Consume) if s.isSuccess() && h.exists(_.name == "X-CM-WELL-POSITION") => {
          val nextPositionKey = res.getHeader("X-CM-WELL-POSITION").get.value()
          logger.info(s"Data Center ID $dataCenterId: Got TSVs stream source. The next position key to consume is $nextPositionKey")
          (entity.dataBytes.mapMaterializedValue(_ => NotUsed), nextPositionKey)
        }
        case (Success(response), _) => {
          //the status isn't success or bad success
          logger.error("bad response: " + response)
//          Future.failed[(Source[ByteString, NotUsed], String)](new Exception("blah blah"))
          ???
        }
        case (Failure(e), _) => {
          logger.error("failure response: ", e)
          ???
        }
      }
    val getTsvGraph = Source.single(request -> Consume).via(flow).toMat(Sink.head)(Keep.right).withAttributes(ActorAttributes.supervisionStrategy(decider))
    getTsvGraph.run() //.flatMapConcat(identity) //- when adding retry, in the flatmap check the Try
  }
}