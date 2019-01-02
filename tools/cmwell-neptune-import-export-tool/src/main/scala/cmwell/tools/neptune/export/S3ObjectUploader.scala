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

object S3ObjectUploader{
  val clientRegion = "us-east-1"
  val bucketName = "cm-well/sync"
  val stringObjKeyName = "cm-well-position.txt"
//  var fullObject: S3Object = _
//  val s3Client = AmazonS3ClientBuilder.standard()
//    .withRegion(clientRegion)
//    .withCredentials(new ProfileCredentialsProvider())
//    .build()
//
//  def persistPositionToS3Bucket(position:String) = {
//
//    try {
//      // Upload a text string as a new object.
//      s3Client.putObject(bucketName, stringObjKeyName, position)
//      // Get an object and print its contents.// Get an object and print its contents.
//    }
//    catch {
//      // The call was transmitted successfully, but Amazon S3 couldn't process
//      // it, so it returned an error response.
//      case e: AmazonServiceException => e.printStackTrace()
//      case e: SdkClientException => e.printStackTrace()
//    }
//
//  }
//
//  def readPositionFromS3Bucket:String = {
//    try {
//      fullObject = s3Client.getObject(new GetObjectRequest(bucketName, stringObjKeyName))
//      displayTextInputStream(fullObject.getObjectContent)
//    }
//    catch {
//      // The call was transmitted successfully, but Amazon S3 couldn't process
//      // it, so it returned an error response.
//      case e: AmazonServiceException => throw new Throwable(e.getMessage)
//      case e: SdkClientException => throw new Throwable(e.getMessage)
//    }
//  }
//
//  def ifFileExists() = {
//    val exists = s3Client.doesObjectExist(bucketName, stringObjKeyName)
//    println("Does " + bucketName + stringObjKeyName + "exists?" + exists)
//    exists
//  }
//
//
//  @throws[IOException]
//  private def displayTextInputStream(input: InputStream): String = { // Read the text input stream one line at a time and display each line.
//    val inputStr = scala.io.Source.fromInputStream(input).mkString
//    println(inputStr)
//    inputStr
//  }

}
