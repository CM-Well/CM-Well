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
package cmwell.tools.neptune.export

import java.io._
import java.util
import java.util.stream.Collectors
import java.util.{Collections, Vector}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.{AmazonServiceException, ClientConfiguration, Protocol, SdkClientException}
import org.apache.commons.io.{FileUtils, IOUtils}

object S3ObjectUploader{


  def init(proxyHost:Option[String], proxyPort:Option[Int]) = {
    val clientRegion = "us-east-1"
    val config = new ClientConfiguration
    config.setProtocol(Protocol.HTTPS)
    proxyHost.foreach(host => config.setProxyHost(host))
    proxyPort.foreach(port =>  config.setProxyPort(port))
    val s3Client = AmazonS3ClientBuilder.standard()
      .withRegion(clientRegion)
      .withClientConfiguration(config)
      .withCredentials(new ProfileCredentialsProvider())
      .build()
    s3Client
  }


  def persistChunkToS3Bucket(chunkData:String, fileName:String, proxyHost:Option[String], proxyPort:Option[Int], s3Directory:String) = {
        try{
          init(proxyHost, proxyPort).putObject(s3Directory, fileName, chunkData)
      }
      catch {
        case e: AmazonServiceException =>
          e.printStackTrace()
          throw e
        case e: SdkClientException =>
          e.printStackTrace()
          throw e
      }
  }

  def persistChunkToS3Bucket(tmpFile:File, fileName:String, proxyHost:Option[String], proxyPort:Option[Int], s3Directory:String) = {
    try{
      init(proxyHost, proxyPort).putObject(s3Directory, fileName, tmpFile)
    }
    catch {
      case e: AmazonServiceException =>
        e.printStackTrace()
        throw e
      case e: SdkClientException =>
        e.printStackTrace()
        throw e
    }
  }

}
