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


package cmwell.tlog

import scala.collection.mutable
import java.lang.String
import java.util.concurrent.atomic.AtomicLong
import scala._
import cmwell.perf.PerfCounter
import com.typesafe.scalalogging.LazyLogging
import scala.Predef._

//import java.util.concurrent._
import scala.concurrent._
import concurrent.duration._

import scala.Some
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import java.io._
import scala.Some
import scala.collection.mutable.ArrayBuffer
import scala.Some
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util.zip.{Adler32, CRC32, Checksum}
import net.jpountz.lz4.{LZ4FastDecompressor, LZ4Compressor, LZ4Factory}
import scala.Some
import ExecutionContext.Implicits.global
import scala.util._
import cmwell.common.metrics.WithMetrics
import com.google.common.cache.{CacheBuilder, Cache}
import scala.language.postfixOps

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 10/30/12
 * Time: 11:11 AM
 * TLog implementation
 */



object TLog {
  def apply(tlogName : String , partitionName : String , cmwell_home_path : String = System.getProperty("cmwell.home"),readOnly : Boolean = false,bucketSize : Long = 1L * 1024L * 1024L * 1024L ) = new TLogFileImpl(tlogName, partitionName.replace('-', '_'), cmwell_home_path, readOnly, bucketSize)
}

trait TLog {
  def name() : String
  def partition() : String
  def init()
  def write(payload : Array[Byte]) : scala.concurrent.Future[Long]
  def write_sync(payload : Array[Byte])
  def read(ts: Long , count : Int ) : (Long , Vector[(Long,Array[Byte])] )
  def read(ts: Long , size : Long ) : (Long , Vector[(Long,Array[Byte])] )
  def readOne(ts:Long):Option[(Long, Array[Byte])]
  def getPartitions : Vector[String]
  def size : Long
  def shutdown()
}

object Header {
  def get = 6074352486920120832L
//  val headerString = "TLogObj\u0000"
//  val arr = java.nio.ByteBuffer.allocate( 8 )
//  arr.put(headerString.getBytes())
//  arr.flip()
//  val headerVal = arr.getLong()
//  def get : Long = headerVal
//  val headerString = "TLogObj".getBytes("utf8")
}

object DataChunk {
  def create(payload : Array[Byte],original_size : Int) = new DataChunk(Header.get , payload, original_size)
}


object Compression {
  val factory : LZ4Factory = LZ4Factory.fastestInstance();

  def compress(payload : Array[Byte]) : Array[Byte] = {
    val compressor : LZ4Compressor = factory.fastCompressor()
    val payload_compress = compressor.compress(payload)
    payload_compress
  }

  def uncompress(payload : Array[Byte] , original_size : Int ) : Array[Byte] = {
    val decompressor : LZ4FastDecompressor = factory.fastDecompressor()
    val payload_uncompress = decompressor.decompress(payload , original_size)
    payload_uncompress
  }
}

/**
 * Header
 *     8       4               4     size         8
 * |header | original size | size | payload | checksum |
 */

/**
 *
 * @param payload
 */
class DataChunk(header : Long , payload : Array[Byte], original_size : Int) {
  val checksum = new Adler32()
  val size = payload.size
  checksum.update(payload, 0 , size)

  def writeData(os:FileOutputStream) = {
    val arr = java.nio.ByteBuffer.allocate( 8 + 8 + 4 + 4 + payload.size + 8)
    // write header
    arr.putLong(header)
    arr.putLong(System.currentTimeMillis())
    arr.putInt(original_size)
    arr.putInt(size)
    arr.put(payload)
    arr.putLong(checksum.getValue)
    arr.flip()
    os.getChannel.write(arr)
  }

  /**
   *
   * @return
   */
  def getHeader : Array[Byte] = {
    val arr = java.nio.ByteBuffer.allocate( 8 + 8 + 4 + 4 )
    arr.putLong(header)
    arr.putLong(System.currentTimeMillis())
    arr.putInt(original_size)
    arr.putInt(size)
    arr.flip()
    arr.array()
  }

  /**
   *
   * @return
   */
  def getPayload : Array[Byte] = payload

  /**
   *
   * @return
   */
  def getCheckSum : Array[Byte] = {
    val arr = java.nio.ByteBuffer.allocate( 8 )
    arr.putLong(checksum.getValue)
    arr.flip()
    arr.array()
  }
}

class DataWriter(dataPath : String , fileName : String , bucketSize : Long) extends LazyLogging {

  def calcFileNumber : Long = {
    val listOfFiles = new File(dataPath).listFiles()
    //println(s"calcFileNumber ${fileName} ${dataPath} ${listOfFiles.mkString("[",",","]")}")

    val l = listOfFiles.filter(_.getName.startsWith(fileName) ).sorted.toList
    if ( l.isEmpty )
      0L
    else {
      val n = l.last.getName.split('_').last
      val nn = n.split('.')
      nn.head.toLong
    }
  }

  // scan directory
  var fileNumber : Long = calcFileNumber

  def buildFileName(dataPath : String , fileName : String , fileNumber : Long) : String = {
    val n= "%09d".format(fileNumber)
    s"${dataPath}${File.separator}${fileName}_${n}.dat"
  }

  var fileNameWithPath = buildFileName( dataPath, fileName , fileNumber )

  var fos : FileOutputStream = new FileOutputStream(fileNameWithPath , true);

  def position : Long = {
    val numOfFiles = calcFileNumber
    val fileNameWithPath = buildFileName( dataPath, fileName , numOfFiles )
    val tmpFos : FileOutputStream = new FileOutputStream(fileNameWithPath , true);
    val size = tmpFos.getChannel.position()
    tmpFos.close()
    ( numOfFiles * bucketSize ) + size
  }

  // this is running on executor actor like
  // THIS IS NOT THREAD SAFE!!!
  def write(payload : Array[Byte] , original_size : Int) : Option[Long] = {
    var offset = fos.getChannel.position()
    //println(s"current offset ${offset} bucketSize ${bucketSize}")
    // check do we need to switch a file
    if ( offset >= bucketSize ) {
      logger info(s"switch file in tlog from ${fileNameWithPath} in offset ${offset}. ")
      try {
        fos.flush()
        fos.close()
      }
      catch {
        case ioe:IOException =>
          println("IOException : " + ioe);
      }
      // first switch a file and make some padding to the new file
      val paddingSize : Long = offset - bucketSize
      fileNumber += 1
      fileNameWithPath = buildFileName( dataPath, fileName , fileNumber )
//      println(s"we need to switch to file ${fileNameWithPath} and make some padding ${paddingSize}.")
      logger info (s"to file ${fileNameWithPath} padding ${paddingSize}. ")
      // switch file
      fos = new FileOutputStream(fileNameWithPath , true);
      // do some padding to the file
      if ( paddingSize != 0 ) {
//        println("try to allocate")
        val arr = java.nio.ByteBuffer.allocate(paddingSize.toInt)
        val v: Byte = 77
//        println("build array allocate")
        (0 to (paddingSize - 1).toInt)
        arr.put(v)
        arr.flip()

        try {
//          println("before write")
          fos.write(arr.array())
//          println("after write")
        }
        catch {
          case ex: FileNotFoundException =>
            println("FileNotFoundException : " + ex);
          case ioe: IOException =>
            println("IOException : " + ioe);
        }
      }
      offset = paddingSize
    }
    // get size
    /*
    val size = payload.size
    checksum.update(payload, 0 , size)
    val checksumValue = checksum.getValue
    checksum.reset()
    val header : Long = 1;
    // pack to byte array
    val arr = java.nio.ByteBuffer.allocate( 8 + 4 + size + 8)
    arr.putLong(header)
    arr.putInt(size)
    arr.put(payload)
    arr.putLong(checksumValue)
    arr.flip()
    */
    try {
      //fos.write(arr.array())
      //arr.clear()
      val d = DataChunk.create(payload,original_size)
      d.writeData(fos)
//      fos.write(d.getHeader)
//      fos.write(d.getPayload)
//      fos.write(d.getCheckSum)
    }
    catch {
      case ex: FileNotFoundException =>
        println("FileNotFoundException : " + ex);
      case ioe: IOException =>
        println("IOException : " + ioe);
    }
    Some(offset)
  }
}

class DataReader(dataPath : String , fileName : String ,bucketSize : Long) extends LazyLogging {

  val fileCacheMap : Cache[String, RandomAccessFile] = CacheBuilder.newBuilder().maximumSize(5).build[String , RandomAccessFile]()

  def buildFileName(dataPath : String , fileName : String , fileNumber : Long) : String = {
    val n = "%09d".format(fileNumber)
    s"${dataPath}${File.separator}${fileName}_${n}.dat"
  }



  def getFileHandel(dataPath : String , fileName : String , fileNumber : Long ) : Option[RandomAccessFile] = {
    val fileNameWithPath = buildFileName( dataPath, fileName , fileNumber )
    val fh = fileCacheMap.getIfPresent(fileNameWithPath)
    if ( fh == null ) {
      // first check if file
      val f = new File(fileNameWithPath)
      if ( f.exists() ) {
        val aFile = new RandomAccessFile(fileNameWithPath, "r");
        fileCacheMap.put(fileNameWithPath, aFile)
        Some(aFile)
      }
      else {
        None
      }
    } else {
      Some(fh)
    }
  }


  val factory : LZ4Factory = LZ4Factory.fastestInstance();

  private def readHeader(offset : Long , inChannel : FileChannel) : Option[Long] = {
    val buf : ByteBuffer = ByteBuffer.allocate(8);
    inChannel.read(buf,offset)
    if ( buf.position != 8 ) {
      //logger.error("header size(%s) is not complete. ".format(buf.position))
      None
    } else {
      buf.flip()
      val header = buf.getLong
      Some(header)
    }
  }

  private def readTimeStamp(offset : Long , inChannel : FileChannel) : Option[Long] = {
    val buf : ByteBuffer = ByteBuffer.allocate(8);
    inChannel.read(buf,offset)
    if ( buf.position != 8 ) {
      //logger.error("timestamp size(%s) is not complete. ".format(buf.position))
      None
    } else {
      buf.flip()
      val timestamp = buf.getLong
      Some(timestamp)
    }
  }

  private def readSize(offset : Long , inChannel : FileChannel) : Option[Int] = {
    val buf = ByteBuffer.allocate(4);
    inChannel.read(buf,offset)
    if ( buf.position != 4 ) {
      //logger.error("size size(%s) is not complete. ".format(buf.position))
      None
    } else {
      buf.flip()
      val size = buf.getInt
      Some(size)
    }
  }

  private def readPayload(offset : Long , size : Int, original_size : Int ,inChannel : FileChannel)  : Option[Array[Byte]] = {
    val buf = ByteBuffer.allocate(size);
    inChannel.read(buf, offset)
    if ( buf.position != size ) {
      //logger.error("payload size(%s) is not complete. ".format(buf.position))
      None
    } else {
      buf.flip()
      //val arr = decompressor.decompress(buf.array() , original_size)
      //println(new String(arr))
      val arr = buf.array()
      Some(arr)
    }
  }

  private def readCheckSum(offset : Long , inChannel : FileChannel) : Option[Long] = {
    val buf = ByteBuffer.allocate(8);
    inChannel.read(buf,offset )
    if ( buf.position != 8 ) {
      //logger.error("checksum size(%s) is not complete. ".format(buf.position))
      None
    } else {
      buf.flip()
      val checksumValue = buf.getLong
      Some(checksumValue)
    }
  }

  //var aFile : RandomAccessFile = new RandomAccessFile(fileNameWithPath, "r")

  // not thread safe

  def read(offset: Long , weight : Long ) : (Long , Vector[(Long,Array[Byte])] ) = {
    var totalWeight = 0L
    // calc the correct buckt to read from

    // create new buffer in size of requested chunk
    val buf : mutable.Buffer[(Long , Array[Byte])] = new mutable.ArrayBuffer[(Long, Array[Byte])](10)
    var stop : Boolean = false
    var newOffset = offset

    while (!stop) {
      val fileNumber = newOffset / bucketSize
      val aFile = getFileHandel(dataPath,fileName,fileNumber)
      aFile match {
        case Some(tlogFileHandel) =>
          val internalFileOffset = newOffset - (fileNumber * bucketSize)
          val item = read_one(internalFileOffset , tlogFileHandel,fileNumber)
          item match {
            case None =>
              // if there is no data stop the while
              stop = true
            case Some(i) =>
              // append data to the item
              val el = ( i._1 + (fileNumber * bucketSize) , i._2 )
              buf.append(i)
              newOffset = el._1
              totalWeight += el._2.size
              if ( totalWeight >= weight)
                stop = true
          }
        case None =>
          stop = true
      }

      //if ( buf.size == size ) stop = true
    }
    if ( buf.isEmpty ) {
      (offset , Vector.empty)
    }
    else {
      val v = Vector.empty[(Long, Array[Byte])] ++ buf
      val l = v.last._1
      ( l , v)
    }


  }


  // not thread safe
  def read(offset: Long,count: Int): (Long, Vector[(Long, Array[Byte])]) = {
//    println(s"read ${ts} ${count} ${fileName} ")
    // create new buffer in size of requested chunk
    val buf : mutable.Buffer[(Long , Array[Byte])] = new mutable.ArrayBuffer[(Long, Array[Byte])](count.toInt)
    var stop : Boolean = false
    var newOffset = offset
    while (!stop) {
      // first try to read one
      val fileNumber = newOffset / bucketSize
      //println(s"${dataPath},${fileName},${fileNumber}")
      val aFile = getFileHandel(dataPath,fileName,fileNumber)
      aFile match {
        case Some(tlogFileHandel) =>
          val internalFileOffset = newOffset - (fileNumber * bucketSize)
          //println(s"${internalFileOffset} ${newTs} ${fileNumber}")
//          if (internalFileOffset == 0 )
//            println(s"aFile ${fileName} newTs ${newTs} fileNumber ${fileNumber} internalFileOffset ${internalFileOffset} count ${count} buf size ${buf.size}" )
          val item = read_one(internalFileOffset, tlogFileHandel,fileNumber)
          item match {
            case None =>
              // if there is no data stop the while
              stop = true
            case Some(i) =>
              // append data to the item
              val el = ( i._1 + (fileNumber * bucketSize) , i._2 )
              buf.append(el)
              //println(s" ${el._1} + ${fileNumber} * ${bucketSize}")
              newOffset = el._1
          }
        case None =>
          stop = true
      }
      if ( buf.size == count.toInt ) stop = true
    }
    if ( buf.isEmpty ) {
      (offset , Vector.empty)
    }
    else {
      val v = Vector.empty[(Long, Array[Byte])] ++ buf
      val l = v.last._1
      ( l , v)
    }

  }

  def readOne(ts:Long):Option[(Long, Array[Byte])] = {
    val d = read(ts,1)
    if ( d._2.isEmpty )
      None
    else
      Some(d._2(0))
  }


  private def read_one(ts : Long , aFile : RandomAccessFile, fileNumber : Long ): Option[(Long, Array[Byte])] = {
    def calcRealOffset(offset : Long) = offset + 1024L * 1024L * 1024L * fileNumber
    try {
      val inChannel : FileChannel = aFile.getChannel();
      if ( ts > inChannel.size() ) {
        None
      } else {
        val checksum : Checksum = new Adler32();

        // *********** patch to fix landing in improper frame. ************

        var hadProblem = false
        var finalTs = ts

        val fileSize = aFile.length()
        while(finalTs <= fileSize && readHeader(finalTs , inChannel).getOrElse(Header.get) != Header.get ) {
          hadProblem = true
          finalTs += 1
        }

        if(hadProblem) {
          logger.error(s"Encountered bad header jumped ${finalTs - ts} bytes. (from ${calcRealOffset(ts)} to ${calcRealOffset(finalTs)})")
        }
        // *****************************************************************
        readHeader(finalTs , inChannel) match {
          case Some(header) =>
            if ( header != Header.get ) {
              logger.error("Header value is not as expected [%s]".format(header))
              //println(s"Header value is not as expected ${ts} ${inChannel.size()} ${fileNumber} ")
              throw new RuntimeException
            } else {
              readTimeStamp( finalTs + 8 , inChannel )  match {
                case None => None
                case Some(timestamp) =>
                  // read the original size
                  //println(timestamp)
                  readSize(finalTs + 8 + 8 , inChannel ) match {
                    case None => None
                    case Some(original_size) =>
                      // read size
                      readSize(finalTs + 8 + 8 + 4 , inChannel ) match {
                        case None => None
                        case Some(size) =>
                          readPayload(finalTs + 8 + 8 + 4 + 4 , size , original_size ,inChannel ) match {
                            case Some(compressed_payload) =>
                              val decompressor : LZ4FastDecompressor = factory.fastDecompressor()
                              val uncompressed_payload = decompressor.decompress(compressed_payload , original_size)

                              //val uncompressed_payload = compressed_payload
                              readCheckSum( finalTs + 8 + 8 + 4 + 4 + size , inChannel ) match {
                                case Some(checkSum) =>
                                  val cs = new Adler32()
                                  cs.update(compressed_payload, 0 , size)
                                  if ( checkSum != cs.getValue ) {
                                    logger.error("checksum is not correct")
                                    val off = finalTs + 8 + 8 + 4 + 4 + size
                                    val d = (off , uncompressed_payload)
                                    Some(d)
                                  } else {
                                    val off = finalTs + 8 + 8 + 4 + 4 + size + 8
                                    val d = (off , uncompressed_payload)
                                    Some(d)
                                  }
                                case None => None
                              }
                            case None => None
                      }
                    }
                  }
              }
            }
          case None => None
        }
      }
    }
    catch {
      case lz4x : net.jpountz.lz4.LZ4Exception => {
        logger.error(s"Couldn't decompress data at offset: ${calcRealOffset(ts)}, skipping to next offset.")
        read_one(ts + 1 , aFile, fileNumber)
      }
      case t : Throwable => throw t
    }
  }


  private def read(ts: Long, aFile : RandomAccessFile): (Long, Vector[(Long, Array[Byte])]) = {
    val inChannel : FileChannel = aFile.getChannel();
    if ( ts >= inChannel.size() ) {
      (ts , Vector.empty)
    } else {
      //
      val checksum : Checksum = new Adler32();
      readHeader(ts , inChannel) match {
        case Some(header) =>
          if ( header != Header.get ) {
            logger.error("Header value is not as expected [%s]".format(header))
            throw new RuntimeException
          } else {
            // read the original size
            readSize(ts + 8 , inChannel ) match {
              case None => (ts , Vector.empty)
              case Some(original_size) =>
                // read size
                readSize(ts + 8 + 4 , inChannel ) match {
                  case None => (ts , Vector.empty)
                  case Some(size) =>
                    readPayload(ts + 8 + 4 + 4 , size , original_size ,inChannel ) match {
                      case Some(compressed_payload) =>
                        val decompressor : LZ4FastDecompressor = factory.fastDecompressor()
                        val uncompressed_payload = decompressor.decompress(compressed_payload , original_size)
                        readCheckSum( ts + 8 + 4 + 4 + size , inChannel ) match {
                          case Some(checkSum) =>
                            val cs = new Adler32()
                            cs.update(compressed_payload, 0 , size)
                            if ( checkSum != cs.getValue ) {
                                logger.error("checksum is not correct")
                                (ts , Vector.empty)
                            } else {
                                val off = ts + 8 + 4 + 4 + size + 8
                                (off , Vector( (off , uncompressed_payload)) )
                            }
                          case None => (ts , Vector.empty)
                        }
                      case None => (ts , Vector.empty)
                    }
                }
              }
            }
        case None => (ts , Vector.empty)
      }
    }
  }

}
/*
class TLogInMemoryBuf(tlogName : String , partitionName : String , homePath : String) extends TLog with LazyLogging {

  val tlog_file = new TLogFileImpl(tlog_file , partitionName , homePath )
  def name() = tlog_file.name()
  def partition() = tlog_file.partition()
  def init() {
    tlog_file.init()
  }
  def write(payload : Array[Byte]) = tlog_file.write(payload)
  def write_sync(payload : Array[Byte]) = tlog_file.write_sync(payload)
  def read(ts: Long , count : Int ) = {

    (Long , Vector[(Long,Array[Byte])] )
  }
  def getPartitions = tlog_file.getPartitions
  def shutdown() {
    tlog_file.shutdown()
  }

}*/

class TLogFileImpl(tlogName : String , partitionName : String , homePath : String , readOnly : Boolean ,bucketSize : Long) extends TLog with LazyLogging /*with WithMetrics*/ {

//  println(s"bucketSize ${bucketSize}")
//  val readTLogTimer  = metrics.meter(s"TLog [$tlogName $partitionName] Read Actions Produced")
//  val writeTLogTimer  = metrics.meter(s"TLog [$tlogName $partitionName] Write Actions Produced")
//  println(s" ${tlogName} ${partitionName} tlog version 1.0")
  val factory : LZ4Factory = LZ4Factory.fastestInstance();
  // dataPath full path to data directory
  val dataPath = homePath + File.separator + "data" + File.separator + "tlog"
  logger.info("data directory for tlog %s".format(dataPath))

  // create directory if not exists
  new File(dataPath).mkdirs()
  // tlog file name
  val fileName = "%s_%s".format(tlogName , partitionName )
  logger.info("tlog filename [%s]".format(fileName))
  // perf
  val perf : PerfCounter = new PerfCounter(tlogName + "." + partitionName,"write:success,write:failure,internal_write:success,internal_write:failure,read:success,read:failure")
  // internal queue to aggregate write operations
  val writeQueue = new LinkedBlockingQueue[ ( Promise[Long], Array[Byte] , Int) ]()
  // define the asyncWriteExcutor internal queue size
  val MAX_QUEUE_SIZE = 10000
  // init executor for internal write operation execution
  val internalProcessingExecutor = Executors.newSingleThreadExecutor()
  // stop boolean in order to stop internal processing thread
  var stop : Boolean = false

  // the writer
  var dataWriter : DataWriter = null
  var dataReader : DataReader = null

  // get operations and execute it via the executor
  def runOnExecutorWriteProcessor(body: => Unit) {
    val r = new Runnable {
      def run = body
    }
    internalProcessingExecutor.submit(r)
  }

  // if readOnly do not start the thread
  if (!readOnly) {
    runOnExecutorWriteProcessor {
      //TODO: optimize: pull more than 1, and write in a bulk
      while (!stop || !writeQueue.isEmpty) {
        val d = writeQueue.poll(1, TimeUnit.SECONDS)
        if (d != null) {
          val p: Promise[Long] = d._1
          val payload: Array[Byte] = d._2
          val original_size = d._3
          internal_write(p, payload, original_size)
        }
      }
    }
  }

  // get current tlog name
  def name() = tlogName
  // get current partition name
  def partition() = partitionName

  def write_sync(payload : Array[Byte]) {
    val f = write(payload)
    val id = Await.result(f,400 seconds )
    //sw.stop("write:success")
  }

  def write(payload : Array[Byte]) : scala.concurrent.Future[Long] = {

    // if readOnly
    if (readOnly)
      ???
    val p = Promise[Long]()

    val compression_f = Future {
      val compressor : LZ4Compressor = factory.fastCompressor()
      val payload_compress = compressor.compress(payload)
      payload_compress
    }

    compression_f onComplete  {
      case Success(payload_compress) =>
        //println("Compression is done.")
        val data = ( p , payload_compress , payload.length )
        //println("Write into q.")
        writeQueue.put(data)
//        writeTLogTimer.mark()
      case Failure(t) =>
        p.failure(t)
    }

    p.future
    //sw.stop("write:success")
  }



  //TODO: bulk writes optimization
  private def internal_write(p : Promise[Long] , payload : Array[Byte] , original_size : Int) {
    val sw = perf.getStopWatch
    dataWriter.write(payload, original_size) match {
      case Some(offset) =>
        sw.stop("internal_write:success")
        p.success(offset)
      case None =>
        sw.stop("internal_write:failure")
        p.success(-1)
    }
  }

  def read(ts: Long , size : Long ) : (Long , Vector[(Long,Array[Byte])] ) = {
    val sw = perf.getStopWatch
    val data = dataReader.read(ts , size)
    sw.stop("read:success")
    data
  }

  def readOne(ts:Long):Option[(Long, Array[Byte])] = {
    val sw = perf.getStopWatch
    val data = dataReader.readOne(ts)
    sw.stop("read:success")
    data
  }

  def read(ts: Long,count: Int): (Long, Vector[(Long, Array[Byte])]) = {
    // here we will make based on calc where we need to route the req for data (what datareader to use)
    val sw = perf.getStopWatch
    val data = dataReader.read(ts , count)
    sw.stop("read:success")
//    readTLogTimer.mark(count)
    data
  }


  def init() {
    dataWriter = new DataWriter(dataPath , fileName , bucketSize)
    dataReader = new DataReader(dataPath , fileName , bucketSize)
  }

  def size : Long = {
    dataWriter.position
  }

  def getPartitions = {
    Vector.empty
  }

  def shutdown() {
    logger.debug("Shutting down TLog [%s-%s]".format(this.name() , this.partition()))
    // only if started to write flush bucket size
    stop = true
    internalProcessingExecutor.shutdown()
    perf.shutdown()
  }

}





