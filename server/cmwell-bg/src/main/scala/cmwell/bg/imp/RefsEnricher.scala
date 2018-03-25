package cmwell.bg.imp

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.contrib.PartitionWith
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import cmwell.bg.{BGMessage, BGMetrics}
import cmwell.common._
import cmwell.zstore.ZStore
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext

object RefsEnricher extends LazyLogging {

  def toSingle(bgm: BGMetrics, irwReadConcurrency: Int, zStore: ZStore)(implicit ec: ExecutionContext): Flow[BGMessage[Command],BGMessage[SingleCommand],NotUsed] = {
    Flow.fromGraph(GraphDSL.create(){ implicit b =>
      import GraphDSL.Implicits._

      // CommandRef goes left, all rest go right
      // update metrics for each type of command
      val commandsPartitioner = b.add(
        PartitionWith[BGMessage[Command],BGMessage[CommandRef],BGMessage[Command]]{
          case bgm@BGMessage(_, CommandRef(_)) => Left(bgm.asInstanceOf[BGMessage[CommandRef]])
          case bgm => Right(bgm)
        }
      )

      val commandRefsFetcher = Flow[BGMessage[CommandRef]].mapAsync(irwReadConcurrency){
        case bgMessage @ BGMessage(_, CommandRef(ref)) =>
          zStore.get(ref).map { payload =>
            bgMessage.copy(message = CommandSerializer.decode(payload))
          }
      }

      val singleCommandsMerge = b.add(Merge[BGMessage[Command]](2))

      commandsPartitioner.out0 ~> commandRefsFetcher ~> singleCommandsMerge.in(0)

      commandsPartitioner.out1 ~> singleCommandsMerge.in(1)

      FlowShape(commandsPartitioner.in, singleCommandsMerge.out.map { bgMessage =>
        // cast to SingleCommand while updating metrics
        bgMessage.message match {
          case wc: WriteCommand =>
            bgm.writeCommandsCounter += 1
            bgm.infotonCommandWeightHist += wc.infoton.weight
          case oc: OverwriteCommand =>
            bgm.overrideCommandCounter += 1
            bgm.infotonCommandWeightHist += oc.infoton.weight
          case _: UpdatePathCommand => bgm.updatePathCommandsCounter += 1
          case _: DeletePathCommand => bgm.deletePathCommandsCounter += 1
          case _: DeleteAttributesCommand => bgm.deleteAttributesCommandsCounter += 1
          case unknown => logger.error(s"unknown command [$unknown]")
        }
        bgm.commandMeter.mark()
        bgMessage.copy(message = bgMessage.message.asInstanceOf[SingleCommand])
      }.outlet)
    })
  }
}
