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


package controllers

import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.joda.time.DateTime
import play.api.http.HeaderNames._
import play.api.http.HttpEntity
import play.api.http.Status._
import play.api.mvc.Results.NotModified
import play.api.mvc._
import wsutil._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

//FIXME: modelled after old code (play 2.3.x) should compare to new code `Assets.at(...)`, and improve!
trait FileInfotonCaching {
  /**
   * copied concepts from Assets.at
   * @see Assets.at
   * @param request
   * @param content
   * @param mime
   * @param path
   * @param uuid
   * @param lastModified
   * @param aggressiveCaching
   * @return
   */
  def treatContentAsAsset(request: Request[_], content: Array[Byte], mime: String, path: String, uuid: String, lastModified: DateTime, aggressiveCaching: Boolean = false): Future[Result] = {
    val etag =
      if(uuid == "0") "\"" + cmwell.util.string.Hash.crc32(content) + "\""
      else "\"" + uuid + "\""

    // cmwell's system file infotons (e.g. SPA), should only use Last-Modified mechanism, and must not be cached in routers or browser.
    // doing so ensures all content will be re-fetched with no delays IN CASE OF AN UPGRADE
    val dontCache = request.path.startsWith("/meta/") || request.path.startsWith("/proc/")

      Assets.assetInfoForRequest(request, path).map(_.map{case (ai,gr) => (ai.gzipUrl.isDefined, gr)}.getOrElse(false -> false)).map {
        case (gzipAvailable, gzipRequested) => {
          maybeNotModified(etag, lastModified, request, false, dontCache = dontCache).getOrElse {
            cacheableResult(
              etag,
              lastModified,
              aggressiveCaching,
              result(content.length, overrideMimetype(mime, request)._2, Source.single(ByteString.fromArray(content)), gzipRequested, gzipAvailable),
              dontCache=dontCache
            )
          }
        }
      }
  }

  def result(length: Long,
             mimeType: String,
             resourceData: Source[ByteString,_],
             gzipRequested: Boolean,
             gzipAvailable: Boolean): Result = {

    //TODO: `HttpEntity.Streamed` or `HttpEntity.Strict` ?
    val entity = HttpEntity.Streamed(resourceData,Some(length),Some(mimeType))
    val response = Result(
      ResponseHeader(
        OK,
        Map(
          CONTENT_LENGTH -> length.toString,
          CONTENT_TYPE -> mimeType,
          DATE -> currentTimeFormatted
        )
      ),
      entity)
    if (gzipRequested && gzipAvailable) {
      response.withHeaders(VARY -> ACCEPT_ENCODING, CONTENT_ENCODING -> "gzip")
    } else if (gzipAvailable) {
      response.withHeaders(VARY -> ACCEPT_ENCODING)
    } else {
      response
    }
  }

  def currentTimeFormatted = ResponseHeader.httpDateFormat.print((new java.util.Date).getTime)

  def maybeNotModified(etag: String, lastModified: DateTime, request: Request[_], aggressiveCaching: Boolean, dontCache: Boolean = false): Option[Result] = {
    // First check etag. Important, if there is an If-None-Match header, we MUST not check the
    // If-Modified-Since header, regardless of whether If-None-Match matches or not. This is in
    // accordance with section 14.26 of RFC2616.
    request.headers.get(IF_NONE_MATCH) match {
      case Some(etags) =>
        Some(etag).filter(someEtag => etags.split(',').exists(_.trim == someEtag)).map(_ => cacheableResult(etag, lastModified, aggressiveCaching, NotModified, dontCache=dontCache))
      case None =>
        getNotModified(request, lastModified.getMillis,dontCache=dontCache)
    }
  }

  def getNotModified(request: Request[_], lastModified: Long, dontCache: Boolean = false): Option[Result] = for {
    ifModifiedSinceStr <- request.headers.get(IF_MODIFIED_SINCE)
    ifModifiedSince = AssetInfo.standardDateParserWithoutTZ.parseDateTime(ifModifiedSinceStr)
    if lastModified < ifModifiedSince.getMillis
  } yield {
    val resp = NotModified.withHeaders(DATE -> currentTimeFormatted)
    if(dontCache) resp.withHeaders(noCacheHeader) else resp
  }

  def cacheableResult[A <: Result](etag: String, lastModified: DateTime, aggressiveCaching: Boolean, r: A, assetInfo: Option[AssetInfo] = None, dontCache: Boolean = false): Result = {

    def addHeaderIfValue(name: String, maybeValue: Option[String], response: Result): Result = {
      maybeValue.fold(response)(v => response.withHeaders(name -> v))
    }

    val r1 = addHeaderIfValue(ETAG, Some(etag), r)
    val r2 = addHeaderIfValue(LAST_MODIFIED, Some(ResponseHeader.httpDateFormat.print(lastModified.getMillis)), r1)

    r2.withHeaders(CACHE_CONTROL -> {assetInfo match {
      case _ if dontCache => noCacheHeader._2
      case Some(ai: AssetInfo) => ai.cacheControl(aggressiveCaching)
      case None if aggressiveCaching => AssetInfo.aggressiveCacheControl
      case None =>  AssetInfo.defaultCacheControl
    }})
  }

  private val noCacheHeader = CACHE_CONTROL -> "public,max-age=360"
}
