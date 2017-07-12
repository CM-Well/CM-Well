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


package cmwell.imp

import java.util.concurrent.TimeUnit._

import cmwell.domain._
import cmwell.irw._
import cmwell.perf.{PerfCounter, ProcessStopWatch}
import cmwell.rts.Publisher
import cmwell.tlog.{TLog, TLogState}
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.common.metrics.WithMetrics
import cmwell.common.{BulkCommand, DeleteAttributesCommand, DeletePathCommand, MergedInfotonCommand, WriteCommand, _}
import cmwell.util.collections._
import cmwell.util.concurrent.travector
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.Predef._
import scala.collection.mutable
import scala.collection.parallel.{ForkJoinTaskSupport, TaskSupport}
import scala.compat.Platform._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 11/27/12
 * Time: 4:58 PM
 * Infoton merge and persist service
 * the service is responsiable for converting tlog commands into infotons ???????????!!!!!
 */


object IMPService {
  def apply(payloadTLog : TLog , uuidTLog : TLog , irw : IRWService , impState : TLogState , parallelism: Int = 4, batchSize : Int = 5) = new IMPServiceImpl(payloadTLog , uuidTLog , irw , impState , parallelism, batchSize)
}

class IMPServiceImpl(payloadTLog : TLog , uuidTLog : TLog , irw : IRWService , impState : TLogState, parallelism: Int = 4, batchSize : Int = 5 ) extends LazyLogging with WithMetrics {

  val rtsEnable : Boolean = true //System.getenv("RTS") != null // todo use a JVM param (-D)
  logger.info("IMPServiceImpl construction")
  val heartbeatLog : Logger = LoggerFactory.getLogger("heartbeat");

  // init performance
  val perf : PerfCounter = new PerfCounter("imp_%s_%s".format(payloadTLog.name() , payloadTLog.partition() ),"write:success,write:failure,delete:success,delete:failure,read:success,read:failure")
  val jobIMPTimer  = metrics.meter("IMP Actions Produced")
  val readTLogTimer  = metrics.meter(s"IMP Read Actions Produced")
  val writeTLogTimer  = metrics.meter(s"IMP Write Actions To Indexer Produced")

  // we are not using the value for a give entry in the cache, but if our library had a set style cache we could use it !!!!!!
  val directoryCacheMap : Cache[String, String] = CacheBuilder.newBuilder().maximumSize(4000 * 10).build[String , String]()
  var stopAfterChunks : Int = -1
  var ts : Long = 0
  var stop : Boolean = false
  // how much time to wait when there is not more transactions to retrieve
  var timeToWait = 1000
  // the amount of transactions to retrieve from cmwell TLog
  var transactionsToRetrieve = 10
  // is finished
  var isFinished = false
  // set taskSupport for parallel use
  val taskSupport : TaskSupport =  new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

  import cmwell.imp.Settings._

  if(loadState){
    impState.loadState match {
      case Some(data) => ts = data
      case None =>
        ts = 0;
        impState.saveState(ts)
    }
  }

  def terminate = {
    impState.saveState(ts)
    stop = true
    // lets check if realy finished the job
    while ( !isFinished ) Thread.sleep(timeToWait)
    logger.info("imp terminated!")
  }

  def process = {
      try {
        internalProcess
      } catch {
        case e: Exception =>
          logger.info("in process" , e)
      }
  }

  /**
   * Accumulate commands from TLog
   *
   * @param ts the offset in tlog where to start accumulate data from.
   * @param max maximum commands to retrieve.
   * @return a vector of commands from tlog.
   */
  private def accumulateData(ts: Long, max: Int): (Long, Vector[Command]) = {
    var timeStamp = ts
    var data = Vector.empty[Command]
    var stop = false
    while ( !stop) {
      val (t , d) = payloadTLog.read(timeStamp , 1)
      val vec : Vector[(Long,Array[Byte])] = d
      logger.debug("retrieve [%d] transactions".format(vec.length))
      val cs = vec map { kv => CommandSerializer.decode(kv._2) }
      val f = (Vector.empty[Command] /: cs) {
        case (vec,BulkCommand(v)) => vec ++ v
        case (vec,cmd) => vec :+ cmd
      }
      data = data ++ f
      logger.debug("accumulate data [%d]".format(data.length))
      if ( data.length > max || t == timeStamp) {
        stop = true
      }
      timeStamp = t
    }
    readTLogTimer.mark(data.size)
    (timeStamp , data )
  }

  val metaNsRegex = "/meta/ns/[^/]+"

  private def internalProcess = {
    logger.info("IMPServiceImpl starting")
    var counter : Int = 0
    var processed : Long = 0
    stop = false
    // load ts from storage if not exists start with the default 0
    while (!stop) {
      // read transactionsToRetrieve elements from tlog
      logger.debug("reading from payloadTLog[%s-%s] from [%d] transactionsToRetrieve[%d]".format(payloadTLog.name() , payloadTLog.partition() , ts , transactionsToRetrieve) )
      val sw = perf.getStopWatch
      val psw : ProcessStopWatch = new ProcessStopWatch("imp")
      var processedPayloads : Long = 0

      val ( timeStamp , f ) = accumulateData(ts,1000)

      if (timeStamp != ts) {
        try {
          logger.debug("Commands to exec [%d]".format(f.length))

          val (overwrites,regular) = f.partition(_.isInstanceOf[OverwriteCommand])
          val owTuples: Iterable[(String,Option[Infoton],Vector[Infoton])] = overwrites.asInstanceOf[Vector[OverwriteCommand]].groupBy(_.infoton.path).map{
            case (path,vec) => {
              //1. an OverwriteCommand guarantees indexTime isn't empty
              //2. on a small vector sort is more efficient than maxBy & filter ?

              //since we had issues with multiple writes of the same infoton, we need to filter these out
              val uniqByUuids = distinctBy(vec)(_.infoton.uuid)
              val uniqInfotons = uniqByUuids.map(_.infoton)

              val sorted = uniqInfotons.sortBy(_.indexTime.get)
              sorted.last match {
                case _: DeletedInfoton => (path,None,sorted)
                case i =>  (path,Some(i),sorted.init)
              }
            }
          }

          val writesParents = owTuples.collect{
            case (path,Some(_),_) => Infoton.getParent(path)
          }

          // we collect only the WriteCommand parents and we converting it to be a set to remove duplicates.
          // ;case UpdatePathCommand(p,d,u) => p
          val paths = regular.collect{
            case WriteCommand(i,trackingID,prevUUID) => i.parent
            case UpdatePathCommand(p,d,u,md,trackingID,prevUUID) if u.nonEmpty => Infoton.getParent(p)
          }.toSet ++ writesParents

          logger.debug(paths.mkString("create parent paths [",",","]"))

          val directoryUuids = {
            val filteredPaths = paths.filterNot(_.matches(metaNsRegex))
            if (paths.nonEmpty) mkdirs(filteredPaths)
            else Vector.empty
          }

          // first we need to extract all delete path commands
          val (deletePathCommand, nonPathCommand) = regular.partition{
            case DeletePathCommand(_, _,trackingID,prevUUID) => true
            case _ => false
          }


          val deletedRootUUIDFut = execute_delete_path_command(deletePathCommand)

          // the bucket size
          /*
          val bucketSize = math.max(nonPathCommand.size / batchSize, 1);
          // here we try to group data by paths
          val g = nonPathCommand.grouped(batchSize).toVector
          logger.debug(s"size nonPathCommand ${nonPathCommand.size} ${g.mkString("[",",","]")}")
          val p = g.par
          p.tasksupport = taskSupport 
          val tmp = p map { v => wrap_execute_command(v, psw) }
          val uuidVec: Vector[(Option[(String,Long)], (String,Long) )] = tmp.flatten.toVector
          logger.debug(s"uuidVec ${uuidVec.mkString("[",",","]")}")
          */


          val sw_read = perf.getStopWatch
          val uuidVec1: Vector[(Option[(String,Long)], (String,Long) )] = excuteCommandsAsyc(nonPathCommand)
          val uuidVec2: Iterable[(Option[(String,Long)], Option[(String,Long,Long)], Vector[(String,Long,Long)])] = executeOverwrites(owTuples)
          sw.stop("write:success",uuidVec1.size)

          //TODO: should inner vector of historic overwrites should be considered in computation?
          processed += uuidVec1.size + uuidVec2.size
          processedPayloads += uuidVec1.size + uuidVec2.size

          jobIMPTimer.mark(uuidVec1.size + uuidVec2.size)

          //TODO: ¯\_(ツ)_/¯
          val deletedRootUUID = Await.result(deletedRootUUIDFut,Duration(10000, MILLISECONDS))

          val uuid_vec = directoryUuids ++ uuidVec1 ++ deletedRootUUID
          logger.debug(s"uuid_vec ${uuid_vec.mkString("[",",","]")}")
          logger.debug(s"uuidVec2 ${uuidVec2.mkString("[",",","]")}")
          uuid_vec.foreach { item =>
            //TODO: convert to par
            val c: Command = MergedInfotonCommand(item._1 , item._2)
            logger.debug("write to uuidTLog [%s-%s] command [%s]".format(uuidTLog.name(), uuidTLog.partition(), c))
            psw.start("uuid CommandSerializer.decode")
            val payload = CommandSerializer.encode(c)
            psw.stop
            psw.start("write uuid tlog")
            uuidTLog.write(payload).onComplete {
              case Success(l) => psw.stop
              case Failure(e) => psw.stop; logger.error(s"failed to write: $c",e)
            }(scala.concurrent.ExecutionContext.Implicits.global) //explicit usage, since importing and allowing implicit usage is dangerous (may be surprising to code owner)
          }
          uuidVec2.foreach { item =>
            val c: Command = OverwrittenInfotonsCommand(item._1 , item._2, item._3)
            logger.debug("write to uuidTLog [%s-%s] command [%s]".format(uuidTLog.name(), uuidTLog.partition(), c))
            psw.start("uuid CommandSerializer.decode")
            val payload = CommandSerializer.encode(c)
            psw.stop
            psw.start("write uuid tlog")
            uuidTLog.write(payload).onComplete {
              case Success(l) => psw.stop
              case Failure(e) => psw.stop; logger.error(s"failed to write: $c",e)
            }(scala.concurrent.ExecutionContext.Implicits.global)
          }
          
          writeTLogTimer.mark(uuid_vec.size + uuidVec2.size)
          logger.debug("save state [%s]".format(timeStamp))
          // write last timeStamp
          psw.start("save state")
          impState.saveState(timeStamp)
          psw.stop
          // set ts to new timeStamp
          ts = timeStamp
          logger.debug("items processed [%s]".format(processedPayloads))
          logger.debug(psw.prettyPrint)
          if (stopAfterChunks != -1) counter += 1
          if (stopAfterChunks == counter) stop = true
        } catch {
          case e: Exception =>
            logger.error(s"skipping timestamp [${ts} - ${timeStamp}]: ${e.getMessage}\n${e.getStackTrace().mkString("", EOL, EOL)}")
            ts = timeStamp
        }
      }
      else {
        sw.stop("read:failure")
        // let's sleep
        heartbeatLog.info("waiting %d ms before polling uuids tLog again. until now [%d] have been processed".format(timeToWait, processed))
        Thread.sleep(timeToWait)
      }

    }
    impState.saveState(ts)
    isFinished = true
  }


  def merge_f(last_fields: Option[Map[String, Set[FieldValue]]], current_fields: Option[Map[String, Set[FieldValue]]]): Option[Map[String, Set[FieldValue]]] = {

    val previousFields = if(last_fields.exists(_.isEmpty)) None else last_fields
    val currentFields = if(current_fields.exists(_.isEmpty)) None else current_fields

    (previousFields, currentFields) match {
      case (None, current)          => current
      case (previous, None)         => previous
      case (Some(last), Some(curr)) => {
        val keys = last.keySet ++ curr.keySet
        val f: Map[String, Set[FieldValue]] = keys.map{ k =>
          val v1 = last.getOrElse(k,Set.empty[FieldValue])
          val v2 = curr.getOrElse(k,Set.empty[FieldValue])
          k -> (v1 ++ v2)
        }(collection.breakOut)
        Some(f)
      }
    }
  }

  def delete_f( current_fields: Option[Map[String, Set[FieldValue]]], deleteFields: Map[String, Set[FieldValue]]): Option[Map[String, Set[FieldValue]]] = {
    val (asterix,filterQuad): (Boolean,Option[String] => Boolean) = deleteFields.get("*") match {
      case Some(quads) if quads.contains(FNull(Some("*"))) => true -> {_ => true}
      case Some(quads) => true -> {quads collect {case FNull(q) => q}}
      case None => false -> {_ => false}
    }
    val delete_fields: Map[String,Set[FieldValue]] = deleteFields.filterKeys(_ != "*")

    current_fields.map (_.map {
      case (k,vs) =>
      //if we have a wild card field delete, we must check every value for it's quad (that's why we added the `|| asterix`)
      if(delete_fields.keySet(k) || asterix) {
        if (delete_fields.get(k).collect{case xs => xs.isEmpty}.getOrElse(false))
          (k, Set.empty[FieldValue])
        else {
          val v = vs.filterNot(fv => filterQuad(fv.quad) || (delete_fields.contains(k) && delete_fields(k).exists{
              case FNull(Some("*")) => true
              case FNull(q) => fv.quad == q
              case fvtd => fvtd.value == fv.value && (fvtd.quad match {
                case None => fv.quad.isEmpty
                case Some("*") => true
                case Some(q) => fv.quad.fold(false)(q.==)
              })
            })
          )
          (k, v)
        }
      }
      else {
        (k,vs)
      }
    } filterNot{case(_,s) => s.isEmpty})
  }

  def update_f( current_fields:Option[Map[String, Set[FieldValue]]] , delete_fields: Map[String , Set[FieldValue]] , add_fields : Map[String , Set[FieldValue]] ) : Option[Map[String,Set[FieldValue]]] = {
    val after_delete = delete_f(current_fields , delete_fields)
    val after_update = merge_f(after_delete,Some(add_fields))
    after_update
  }

  def delete_merge(prev_infoton: Infoton, fields: Map[String, Set[FieldValue]], lastModified: DateTime): Infoton = {
    prev_infoton match {
      case ObjectInfoton(path,dc,_,_,current_fields,_) =>
        val newFields = delete_f(current_fields, fields )
        if (newFields.nonEmpty) ObjectInfoton(path, dc, None, lastModified, newFields)
        else DeletedInfoton(path, dc, None, lastModified)
      case f@FileInfoton(_,_,_,_,current_fields,_,_) =>
        f.copy(indexTime = None, lastModified = lastModified, fields = delete_f(current_fields, fields ))
      case l@LinkInfoton(_,_,_,_,current_fields,_,_,_) =>
        l.copy(indexTime = None, lastModified = lastModified, fields = delete_f( current_fields,fields ))
      case i: DeletedInfoton => i // if we got a delete on delete we need ignore the create of delete
      case j =>
        logger.warn(s"kind [${j.kind}] uuid [${prev_infoton.uuid}] info [$j]")
        ???
    }
  }

  def update_merge(current_infoton: Infoton, delete_fields: Map[String, Set[FieldValue]], add_fields: Map[String, Set[FieldValue]], lastModified: DateTime): Infoton = {
    val u_f = update_f(current_infoton.fields , delete_fields , add_fields)
    current_infoton match {
      case ObjectInfoton(path, dc, idxT, lm, current_fields,_) if u_f.exists(_.nonEmpty) =>
        ObjectInfoton(path, dc, None, lastModified, u_f )
      case ObjectInfoton(path, dc, idxT, lm, current_fields,_) =>
        DeletedInfoton(path, dc, None, lastModified)
      case FileInfoton(path, dc, idxT, lm, current_fields , c_fc,_) =>
        FileInfoton(path, dc, None, lastModified , u_f , c_fc)
      case LinkInfoton(path, dc, idxT, lm, current_fields, c_to, c_linkType,_) =>
        LinkInfoton(path, dc, None, lastModified, u_f , c_to, c_linkType)
      case DeletedInfoton(path ,dc, idxT, lm ,_) if u_f.exists(_.nonEmpty) =>
        // if we got update after a delete infoton we create a new one
        ObjectInfoton(path, dc, None, lastModified, u_f )
      case _ =>
        // might happen when e.g: writing a "skeleton" on top of a deleted infoton.
        logger.warn(s"kind [${current_infoton.kind}] uuid [${current_infoton.uuid}] info [$current_infoton]")
        current_infoton
    }
  }

  def write_merge(prev_infoton: Infoton, current_infoton: Infoton): Infoton = {
    // we build the new infoton based on the old one first we need to merge the fields
    current_infoton match {
      case ObjectInfoton(path, dc, _, lastModified, current_fields,_) =>
        prev_infoton match {
          case ObjectInfoton(_, _, _, _, prev_fields,_) =>
            ObjectInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields))
          case FileInfoton(_, _, _, _ , prev_fields, perv_fc,_) =>
            FileInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields), perv_fc)
          case LinkInfoton(_, _, _, _, prev_fields, prev_to, prev_linkType,_) =>
            LinkInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields), prev_to, prev_linkType)
          case DeletedInfoton(_, _, _, _,_) =>
            current_infoton
          case _ =>
            ???
        }
      case FileInfoton(path, dc, _, lastModified, current_fields , c_fc,_) =>
        prev_infoton match {
          case ObjectInfoton(_, _, _, _, prev_fields,_) =>
            FileInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields ), c_fc)
          case FileInfoton(_, _, _, _, prev_fields, prev_fc,_) =>
            FileInfoton(path, dc, None, lastModified, merge_f(if(prev_fields.exists(_.nonEmpty)) prev_fields else None, current_fields), c_fc.orElse(prev_fc))
          case LinkInfoton(_, _, _, _, prev_fields, _, _,_) if prev_fields.exists(_.nonEmpty) =>
            FileInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields), c_fc)
          case _: LinkInfoton | _: DeletedInfoton =>
            current_infoton
          case _ =>
            ???

        }
      case LinkInfoton(path, dc, _, lastModified, current_fields, c_to, c_linkType,_) =>
        prev_infoton match {
          case ObjectInfoton(_, _, _, _, prev_fields,_) =>
            LinkInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields), c_to, c_linkType)
          case LinkInfoton(_, _, _, _, prev_fields, _, _,_) =>
            LinkInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields), c_to, c_linkType)
          case FileInfoton(_, _, _, _, prev_fields, _,_) if prev_fields.exists(_.nonEmpty) =>
            LinkInfoton(path, dc, None, lastModified, merge_f(prev_fields, current_fields), c_to, c_linkType)
          case _ : DeletedInfoton | _: FileInfoton =>
            current_infoton
          case _ =>
            ???
        }
      case _ =>
        ???
    }
  }

  private def createParentDirecotryVec(noDupPaths : Set[String]) : Vector[Infoton] = {
    if ( noDupPaths.isEmpty )
      Vector.empty
    else {
      val tmp = noDupPaths.filter{ f=> !f.isEmpty }
      val p = irw.exists(tmp,QUORUM)
      val ( pathNotExists , pathExists ) = p.partition(_._2.isEmpty)
      var uuidData = new mutable.ArrayBuffer[Infoton]()

      // first lets create Drirectory infoton for all pathNotExists
      if ( pathNotExists.nonEmpty ) {                                          // TODO: why not use: `new DateTime(0L)` ?
      val i = pathNotExists map { case (path , value ) => ObjectInfoton(path,  Settings.dataCenter, None, new DateTime(1970,1,1,0,0,0), Map.empty[String, Set[FieldValue]]) }
        val v = i.toVector
        var parents = new mutable.ArrayBuffer[String]()
        v.foreach{ directoryInfoton =>  uuidData +=  directoryInfoton  }
        v.foreach{ directoryInfoton => if ( directoryInfoton.parent != "$root" ) parents += directoryInfoton.parent }
        val tmp = mutable.ArrayBuffer.empty ++ createParentDirecotryVec(parents.toSet)
        uuidData = uuidData ++ tmp
      }
      Vector.empty ++ uuidData
    }
  }

  private def mkdirs(noDupPaths : Set[String]) : Vector[(Option[(String,Long)] , (String,Long) )] = {
    val parentVec = createParentDirecotryVec(noDupPaths)
    logger.debug(s"parentVec [${parentVec}]")
    val r = Future.traverse(parentVec) {
      parent =>
        irw.writeAsync(parent, QUORUM)
    }
    val directoriesVec = Await.result(r,Duration(10000, MILLISECONDS))
    val res = directoriesVec.map { directoryInfoton => None -> ( directoryInfoton.uuid -> directoryInfoton.weight )}
    res

  }

  // TODO: need to read in bulk and write in bulk
  def execute_delete_path_command(cmdVec : Vector[Command]) : Future[Vector[(Option[(String,Long)],(String,Long))]] = {
    travector(cmdVec.collect{case DeletePathCommand(path, lastModified, trackingID, prevUUID) => path -> lastModified}) {
      case (path,lastModified) => {
        irw.readPathAsync(path,QUORUM).flatMap {
          case FullBox(last_infoton) =>
            last_infoton match {
              case _: DeletedInfoton => Future.successful(None)
              case _ => {
                // lets create delete infoton
                val deletedInfotonRoot = DeletedInfoton(path, Settings.dataCenter, None, lastModified)
                irw.writeAsync(deletedInfotonRoot,QUORUM).map { _ =>
                  Some((Some(last_infoton.uuid, last_infoton.weight), (deletedInfotonRoot.uuid, deletedInfotonRoot.weight)))
                }
              }
            }
          // lets return empty vector so we will no do anything
          case EmptyBox => Future.successful(None)
          //FIXME: throwing an exception here will result in a failed future. perhaps we should propagate `Box` type?
          case BoxedFailure(e) => throw new RuntimeException(s"execute_delete_path_command failed for [$path] and [${cmwell.util.string.dateStringify(lastModified)}]",e)
        }
      }
    }.map(_.collect{case Some(tlog2cmd) => tlog2cmd})
  }

  def commandLogic(infoton : Option[Infoton] , cmd : Command) : Option[Infoton] = {
    cmd match {
      case WriteCommand(currInfoton,trackingID,prevUUID) =>
        infoton match {
          case Some(last_infoton) =>
            val merged_infoton : Infoton = write_merge(last_infoton , currInfoton )
            if (!merged_infoton.isSameAs(last_infoton)) {
              Some(merged_infoton)
            } else {
              logger.debug("got the same infoton will not add to merge [%s]".format(merged_infoton.path))
              None
            }
          case None =>
            // no prev infoton no need to merge lets get the data for the infoton from the WriteCommand(i)
            Some(currInfoton)
        }
      case DeleteAttributesCommand(path,fields,lastModified,trackingID,prevUUID) =>
        infoton match {
          case Some(last_infoton) =>
            val merged_infoton : Infoton = delete_merge(last_infoton , fields, lastModified)
            if (!merged_infoton.isSameAs(last_infoton)) {
              Some(merged_infoton)
            } else {
              logger.debug("got the same infoton will not add to merge [%s]".format(merged_infoton.path))
              None
            }
          case None =>
            None
        }
      case UpdatePathCommand(path , deleteFields, updateFields ,lastModified,trackingID,prevUUID) =>
        infoton match {
          case Some(last_infoton) =>
            // here we need to update the
            val updated_infoton : Infoton = update_merge(last_infoton, deleteFields, updateFields, lastModified )
            if (!updated_infoton.isSameAs(last_infoton)) {
              Some(updated_infoton)
            } else {
              logger.debug("got the same infoton will not add to merge [%s]".format(updated_infoton.path))
              None
            }
          case None => // no infoton need so i will create a new one
            val new_infoton = ObjectInfoton(path, Settings.dataCenter, None, lastModified, updateFields)
            Some(new_infoton)
        }
      case _ => ??? //UnimplementedException is better than unexhaustive match
    }
  }

  def excuteCommand(path : String , cmdVec : Vector[Command] ) : Future[Option[(Option[(String,Long)],(String,Long))]] = {
    // first lets read using irw by path
    val p = Promise[Option[(Option[(String,Long)],(String,Long))]]()
    // try to read last uuid from path
    val uuidFuture = irw.readPathUUIDA(path,QUORUM)
    uuidFuture.onComplete{
      // we got the uuid
      case Success(uuid) =>
        uuid match {
          case FullBox(u) =>
            val infotonFuture = irw.readUUIDAsync(u,QUORUM)
            infotonFuture.onComplete{
              //FIXME: throwing an exception here will result in a failed future.
              case Success(BoxedFailure(e)) => throw new RuntimeException(s"readUUIDAsync failed for [$u]",e)
              case Success(b) =>
                val i: Option[Infoton] = b.toOption
                val prevInfotonOption = i
                var currInfotonOption = i
                cmdVec.foreach{
                  cmd => currInfotonOption = commandLogic(currInfotonOption,cmd)
                }
                // now we need to write
                if ( currInfotonOption.isDefined ) {
                  val wI = currInfotonOption.get
                  irw.writeAsync(wI,QUORUM).onComplete {
                    case Success(b) =>
                      val res = prevInfotonOption match {
                        case Some(pI) =>
                          val r = (Option(pI.uuid, pI.weight), (wI.uuid, wI.weight))
                          Some(r)
                        case None =>
                          val r = (None, (wI.uuid, wI.weight))
                          Some(r)
                      }

                      if(rtsEnable) // todo rtsEnable && Grid.membersVerbose.exists(_.roles.exists("subscriber"))
                        Publisher.publish(Vector(wI))

                      p.success(res)
                    case Failure(t) =>
                      p.failure(t)
                    }
                } else {
                  // because we do not need to update infoton we are return ing none
                  p.success(None)
                }
              case Failure(t) =>
                p.failure(t)
            }
          // we could not extract data from path so probably the infotone was not created yet so we need to create a new one
          case EmptyBox =>
            var currInfotonOption : Option[Infoton] = None
            cmdVec.foreach{
              cmd => currInfotonOption = commandLogic(currInfotonOption,cmd)
            }
            // now we need to write
            if ( currInfotonOption.isDefined ) {
              val wI = currInfotonOption.get
              irw.writeAsync(wI,QUORUM).onComplete {
                case Success(b) =>
                  val r = (None, (wI.uuid, wI.weight))
                  if(rtsEnable) // todo rtsEnable && Grid.membersVerbose.exists(_.roles.exists("subscriber"))
                    Publisher.publish(Vector(wI))
                  p.success(Some(r))

                case Failure(t) =>
                  p.failure(t)
              }
            } else {
              // because we do not need to update infoton we are return ing none
              p.success(None)
            }
          case BoxedFailure(e) => p.failure(new RuntimeException(s"readPathUUIDA failed for [$path]",e))
        }
      // failed to read we probably have exception
      case Failure(t) =>
        p.failure(t)
    }
    p.future
  }

  def executeOverwrites(ow: Iterable[(String,Option[Infoton],Vector[Infoton])]): Iterable[(Option[(String,Long)], Option[(String,Long,Long)],Vector[(String,Long,Long)])] = {

    //TODO: can macros help deal with this code duplication?
    def vec2tups(vec: Vector[Infoton]): Vector[(String,Long,Long)] = vec.map(infoton => (infoton.uuid,  infoton.weight, infoton.indexTime.get))
    def opt2tups(opt: Option[Infoton]): Option[(String,Long,Long)] = opt.map(infoton => (infoton.uuid,  infoton.weight, infoton.indexTime.get))

    val f = Future.traverse(ow){
      case t@(path,iOpt,vec) => {
        require(iOpt.forall(_.indexTime.isDefined),"current must have indexTime")
        require(vec.forall(_.indexTime.isDefined),"histories must have indexTime")

        val prevFut = irw.readPathUUIDA(path, QUORUM).flatMap {
          case FullBox(uuid) => irw.readUUIDAsync(uuid,QUORUM).map {
            //FIXME: possible errors are discarded!
            case BoxedFailure(e) =>
              logger.error(s"discarding error from failed readUUIDAsync with uuid [$uuid]",e)
              None
            case b => b.toOption
          }
          case EmptyBox => Future.successful(None)
          case BoxedFailure(e) => Future.failed(e)
        }.recover{
          case e: Throwable => {
            logger.error(s"Exception for tuple: $t",e)
            None
          }
        }

        prevFut.flatMap{ prevOpt =>

          def mkFutureRes(p: Option[Infoton], c: Option[Infoton], v: Vector[Infoton], skipSetPathLast: Boolean = false) = {
            val f = {
              if (c.isEmpty) {
                val newest = v.maxBy(i => i.indexTime.getOrElse(i.lastModified.getMillis))
                val fut = {
                  val f1 = Future.traverse(v.filter(_ != newest))(irw.writeAsync(_,QUORUM,skipSetPathLast=true))
                  val f2 = irw.writeAsync(newest,QUORUM,skipSetPathLast)
                  f2.flatMap(j => f1.map(_ :+ j))
                }
// TODO:
//                fut.onComplete{ t =>
//                  //DO I NEED TO CLEAR IRW's CACHE FOR THIS PATH?
//                  //CHECK WHAT HAPPENS IN REGULAR DELETES
//                }
                fut
              }
              else if (v.isEmpty) irw.writeAsync(c.get,QUORUM,skipSetPathLast).map(Vector(_))
              //making sure the last infoton being written to irw is the current
              //since we want it to be the infoton in the irw cache
              else {
                val f = irw.writeAsync(c.get,QUORUM,skipSetPathLast)
                Future.traverse(v)(irw.writeAsync(_,QUORUM,skipSetPathLast=true)).flatMap(u => f.map(_ +: u))
              }
            }

            f.map { _ =>
              Some((p.map(i => i.uuid -> i.weight), opt2tups(c), vec2tups(v)))
            }
          }

          def filterExistingHistories(hVec: Vector[Infoton]): Future[Vector[Infoton]] = {
            Future.traverse(hVec){ i =>
              irw.readUUIDAsync(i.uuid,QUORUM).map(_ -> i)
            }.map(_.collect{
              case (EmptyBox,i) => i
            })
          }

          val prevIndexTime = prevOpt.flatMap(_.indexTime)

          //make sure to avoid null updates
          if(iOpt.exists(i => prevOpt.exists(_.uuid == i.uuid)) && vec.isEmpty) {
            logger.debug(s"IMPService: iOpt.exists(i => prevOpt.exists(_.uuid == i.uuid)) && vec.isEmpty => (${iOpt.map(_.path)} , ${iOpt.map(_.uuid)})")
            Future.successful(None)
          }
          //if everything we have is newer than what is already stored in cassandra, just write everything
          else if (prevIndexTime.isEmpty || vec.forall(_.indexTime.get > prevIndexTime.get)) {
            logger.debug(s"IMPService: prevIndexTime.isEmpty || vec.forall(_.indexTime.get > prevIndexTime.get) => (${iOpt.map(_.path)} , ${iOpt.map(_.uuid)})")
            Try(mkFutureRes(prevOpt, iOpt, vec)).recoverWith {
              case err: Throwable =>
                logger.error(s"exception when evaluating (1): mkFutureRes($prevOpt, $iOpt, $vec)", err)
                Failure(err)
            }.get
          }
          //if some stuff is newer, some is older, filter by trying to retrieve only what is necessary
          else if ((iOpt.isDefined && prevIndexTime.get < iOpt.get.indexTime.get) ||
            (iOpt.isEmpty && vec.exists(_.indexTime.get >= prevIndexTime.get))) {
            logger.debug(s"IMPService: (iOpt.isDefined && prevIndexTime.get < iOpt.get.indexTime.get => (${iOpt.map(_.path)} , ${iOpt.map(_.uuid)})")
            val (newHistories, oldHistories) = vec.partition(_.indexTime.get > prevIndexTime.get) //prevIndexTime.get is safe because if it were empty we would enter the previous condition in "if" statement

            filterExistingHistories(oldHistories).flatMap { missingHistories =>
              val hVec = newHistories ++ missingHistories
              if(hVec.isEmpty && iOpt.isEmpty) Future.successful(None)
              else Try(mkFutureRes(prevOpt, iOpt, hVec)).recoverWith {
                case err: Throwable =>
                  logger.error(s"exception when evaluating (2): mkFutureRes($prevOpt, $iOpt, $hVec)",err)
                  Failure(err)
              }.get
            }
          }
          //everything, including the current, is either older, or same "age" as the previous
          else {
            logger.debug(s"IMPService: ELSE => (${iOpt.map(_.path)} , ${iOpt.map(_.uuid)})")
            filterExistingHistories(vec ++ iOpt).flatMap { missingHistories =>
              if(missingHistories.isEmpty) Future.successful(None)
              else {
                //we don't want to change current, just write some histories...
                Try(mkFutureRes(None, None, missingHistories, skipSetPathLast = true)).recoverWith {
                  case err: Throwable =>
                    logger.error(s"exception when evaluating (3): mkFutureRes(N,N, $missingHistories, skipSetPathLast = true)", err)
                    Failure(err)
                }.get
              }
            }
          }
        }
      }
    }.map(_.collect{case Some(t) => t})

    lazy val emptyRV = Iterable.empty[(Option[(String,Long)],Option[(String,Long,Long)],Vector[(String,Long,Long)])]

    Try(Await.result(f.recover{
      case ex: Throwable =>
        logger.error(s"got exception: ${ex.getMessage}, ow: $ow",ex)
        emptyRV
    },Duration(20000, MILLISECONDS))) match {
      case Success(it) => it
      case Failure(ex) => {
        logger.error(s"timeout in Await for result in executeOverwrites, ow: $ow", ex)
        emptyRV
      }
    }
  }

  def excuteCommandsAsyc(cmdVec : Vector[Command]) : Vector[(Option[(String,Long)] , (String,Long) )] = {
    logger.debug(s"cmdVec: ${cmdVec.size} --- ${cmdVec}")
    // first lets group by path the command so if there is a path with a few commands we could do all commands in one pass (one read and write from cassandra)
    val paths : Map [String , Vector[Command] ] = cmdVec groupBy {
      case WriteCommand(i,trackingID,prevUUID) => i.path
      case DeleteAttributesCommand(path,f,lm,trackingID,prevUUID) => path
      case UpdatePathCommand(path,f_d,f_u,lm,trackingID,prevUUID) => path
      case e => {
        logger.error(s"can not groupby this ${e}")
        ""
      }
    }
    logger.debug(s"going to work on ${paths.mkString("[","-","]")}")
    // lets excute commands
    val fr = Future.traverse(paths) {
      case (p,cmds) =>
        excuteCommand(p,cmds.distinct)
    }
    val frWithRecover = fr recover  {
      case e : Throwable =>
        logger.error(s"got exception: ${e.getMessage}",e)
        Vector.empty
    }
    logger.debug(s"lets wait until all Futures will return.")
    // now make a blocking call until for all the results will return.
    val uuids = Try(Await.result(frWithRecover,Duration(10000, MILLISECONDS))) match {
      case Success(uu) => uu
      case Failure(t) =>
        logger.error(s"timeout in Await for result in excuteCommandsAsyc: ${t.getMessage}.",t)
        Vector.empty
    }
    uuids.collect{ case Some(c) => c }.toVector
  }

}

object Settings {
  val config = ConfigFactory.load()

  val loadState:Boolean = config.getBoolean("impService.loadState")
  val dataCenter = config.getString("dataCenter.id")
}
