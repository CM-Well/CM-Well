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
package controllers

import java.time.{Instant, LocalDateTime, ZoneId}

import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.util.string.Hash.xxhash32
import cmwell.util.numeric.Radix64.encodeUnsigned
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
abstract class FileInfotonCaching(defaultAssetsMetadata: DefaultAssetsMetadata,
                                  assetsConfiguration: AssetsConfiguration) {

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
  def treatContentAsAsset(request: Request[_],
                          content: Array[Byte],
                          mime: String,
                          path: String,
                          uuid: String,
                          lastModified: DateTime,
                          aggressiveCaching: Boolean = false): Future[Result] = {
    val etag =
      if (uuid == "0") "\"" + encodeUnsigned(xxhash32(content)) + "\""
      else "\"" + uuid + "\""

    // cmwell's system file infotons (e.g. SPA), should only use Last-Modified mechanism, and must not be cached in routers or browser.
    // doing so ensures all content will be re-fetched with no delays IN CASE OF AN UPGRADE
    val dontCache = request.path.startsWith("/meta/") || request.path.startsWith("/proc/")

    defaultAssetsMetadata.assetInfoForRequest(request, path).map { aiOpt =>
      val (gzipRequested, gzipAvailable) = aiOpt.fold(false -> false) { ai =>
        ai._2.accepts("gzip") -> ai._1.compressedUrls.exists(_._1.endsWith(".gz"))
      }
      maybeNotModified(etag, lastModified, request, aggressiveCaching = false, dontCache = dontCache).getOrElse {
        cacheableResult(
          etag,
          lastModified,
          aggressiveCaching,
          result(content.length,
                 overrideMimetype(mime, request)._2,

            // We used to have:  Source.single(ByteString.fromArray(content)),
            // but there's a bug in Play where it terminates connection too early for slow connections if the response is an Array and not Iterator
            // See https://gist.github.com/bryaakov/da0df0b825c8370a46b7f058279dec45
                 Source.fromIterator(() => content.grouped(1024).map(ByteString(_))),

                 gzipRequested,
                 gzipAvailable),
          dontCache = dontCache
        )
      }
    }
  }

  def result(length: Long,
             mimeType: String,
             resourceData: Source[ByteString, _],
             gzipRequested: Boolean,
             gzipAvailable: Boolean): Result = {

    //TODO: `HttpEntity.Streamed` or `HttpEntity.Strict` ?
    val entity = HttpEntity.Streamed(resourceData, Some(length), Some(mimeType))
    val response = Result(ResponseHeader(OK, Map(DATE -> currentTimeFormatted)), entity).as(mimeType)
    if (gzipRequested && gzipAvailable) {
      response.withHeaders(VARY -> ACCEPT_ENCODING, CONTENT_ENCODING -> "gzip")
    } else if (gzipAvailable) {
      response.withHeaders(VARY -> ACCEPT_ENCODING)
    } else {
      response
    }
  }

  def currentTimeFormatted = java.time.LocalDateTime.now().format(ResponseHeader.httpDateFormat)

  def maybeNotModified(etag: String,
                       lastModified: DateTime,
                       request: Request[_],
                       aggressiveCaching: Boolean,
                       dontCache: Boolean = false): Option[Result] = {
    // First check etag. Important, if there is an If-None-Match header, we MUST not check the
    // If-Modified-Since header, regardless of whether If-None-Match matches or not. This is in
    // accordance with section 14.26 of RFC2616.
    request.headers.get(IF_NONE_MATCH) match {
      case Some(etags) =>
        Some(etag)
          .filter(someEtag => etags.split(',').exists(_.trim == someEtag))
          .map(_ => cacheableResult(etag, lastModified, aggressiveCaching, NotModified, dontCache = dontCache))
      case None =>
        getNotModified(request, lastModified.getMillis, dontCache = dontCache)
    }
  }

  def getNotModified(request: Request[_], lastModified: Long, dontCache: Boolean = false): Option[Result] =
    for {
      ifModifiedSinceStr <- request.headers.get(IF_MODIFIED_SINCE)
      ifModifiedSince = Assets.parseModifiedDate(ifModifiedSinceStr)
      if ifModifiedSince.fold(true)(lastModified < _.getTime)
    } yield {
      val resp = NotModified.withHeaders(DATE -> currentTimeFormatted)
      if (dontCache) resp.withHeaders(noCacheHeader) else resp
    }

  def cacheableResult[A <: Result](etag: String,
                                   lastModified: DateTime,
                                   aggressiveCaching: Boolean,
                                   r: A,
                                   assetInfo: Option[AssetsConfiguration] = None,
                                   dontCache: Boolean = false): Result = {

    def addHeaderIfValue(name: String, maybeValue: Option[String], response: Result): Result = {
      maybeValue.fold(response)(v => response.withHeaders(name -> v))
    }

    val r1 = addHeaderIfValue(ETAG, Some(etag), r)
    val d = LocalDateTime
      .ofInstant(Instant.ofEpochMilli(lastModified.getMillis), ZoneId.systemDefault())
      .format(ResponseHeader.httpDateFormat)
    val r2 = addHeaderIfValue(LAST_MODIFIED, Some(d), r1)

    r2.withHeaders(CACHE_CONTROL -> {
      if (aggressiveCaching) assetsConfiguration.aggressiveCacheControl
      else assetsConfiguration.defaultCacheControl
    })
  }

  private val noCacheHeader = CACHE_CONTROL -> "public,max-age=360"
}
