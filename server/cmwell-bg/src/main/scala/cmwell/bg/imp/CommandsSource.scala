package cmwell.bg.imp

import java.nio.charset.StandardCharsets

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.{KillSwitch, KillSwitches, SourceShape}
import akka.stream.scaladsl.{GraphDSL, Keep, MergePreferred, Source}
import cmwell.bg.{BGMessage, CompleteOffset}
import cmwell.common.{Command, CommandSerializer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Proj: server
  * User: gilad
  * Date: 2/20/18
  * Time: 3:52 PM
  */
object CommandsSource extends LazyLogging {

  def fromKafka(persistCommandsTopic: String,
          bootStrapServers: String,
          partition: Int,
          startingOffset: Long,
          startingOffsetPriority: Long)
         (implicit actorSystem: ActorSystem,
                   executionContext: ExecutionContext): (Source[BGMessage[Command],Consumer.Control], KillSwitch) = {

    val byteArrayDeserializer = new ByteArrayDeserializer()
    val persistCommandsTopicPriority = persistCommandsTopic + ".priority"

    val sharedKillSwitch = KillSwitches.shared("persist-sources-kill-switch")

    val subscription = Subscriptions.assignmentWithOffset(
      new TopicPartition(persistCommandsTopic, partition) -> startingOffset)

    val persistCommandsConsumerSettings =
      ConsumerSettings(actorSystem, byteArrayDeserializer, byteArrayDeserializer)
        .withBootstrapServers(bootStrapServers)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val prioritySubscription = Subscriptions.assignmentWithOffset(
      new TopicPartition(persistCommandsTopicPriority, partition) -> startingOffsetPriority)

    val persistCommandsSource = Consumer.plainSource[Array[Byte], Array[Byte]](persistCommandsConsumerSettings, subscription).map { msg =>
      logger.debug(s"consuming next payload from persist commands topic @ ${msg.offset()}")
      val commandTry = Try(CommandSerializer.decode(msg.value()))
      commandTry.failed.foreach { err =>
        logger.error(s"deserialize command error for msg [$msg] and value: [${new String(msg.value(),StandardCharsets.UTF_8)}]",err)
      }
      val command = commandTry.get
      logger.debug(s"consumed command: $command")
      BGMessage[Command](CompleteOffset(msg.topic(), msg.offset()), command)
    }.via(sharedKillSwitch.flow)

    val priorityPersistCommandsSource = Consumer.plainSource[Array[Byte], Array[Byte]](persistCommandsConsumerSettings, prioritySubscription).map { msg =>
      logger.info(s"consuming next payload from priority persist commands topic @ ${msg.offset()}")
      val commandTry = Try(CommandSerializer.decode(msg.value()))
      commandTry.failed.foreach { err =>
        logger.error(s"deserialize command error for msg [$msg] and value: [${new String(msg.value(),StandardCharsets.UTF_8)}]",err)
      }
      val command =commandTry.get
      logger.info(s"consumed priority command: $command")
      BGMessage[Command](CompleteOffset(msg.topic(), msg.offset()), command)
    }.via(sharedKillSwitch.flow)

    val g = GraphDSL.create(priorityPersistCommandsSource, persistCommandsSource)(combineConsumerControls)(implicit builder => (prioritySource, source) => {

      import GraphDSL.Implicits._

      val mergePreferedSources = builder.add(
        MergePreferred[BGMessage[Command]](1, true)
      )

      prioritySource ~> mergePreferedSources.preferred

      source ~> mergePreferedSources.in(0)

      SourceShape(mergePreferedSources.out)
    })

    Source.fromGraph(g) -> sharedKillSwitch
  }

  def combineConsumerControls(left: Consumer.Control, right: Consumer.Control)(implicit ec: ExecutionContext): Consumer.Control = new Consumer.Control {
    override def stop(): Future[Done] = {
      val l = left.stop()
      val r = right.stop()
      for {
        _ <- l
        _ <- r
      } yield Done
    }

    override def shutdown(): Future[Done] = {
      val l = left.shutdown()
      val r = right.shutdown()
      for {
        _ <- l
        _ <- r
      } yield Done
    }

    override def isShutdown: Future[Done] = {
      val l = left.isShutdown
      val r = right.isShutdown
      for {
        _ <- l
        _ <- r
      } yield Done
    }
  }
}
