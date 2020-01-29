# Function: *Consume Next Chunk* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.CreateConsumer.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Subscribe.SubscribeForPushedData.md)  

----

## Description ##
If you wish to retrieve a large number of infotons, but you want to iterate over small "chunks" of data in a controlled fashion, you can use the **create-consumer** API and the `_consume` endpoint. 

The process requires two different API calls:
1. Call **create-consumer** to receive a position identifier (in the **X-CM-WELL-POSITION** header).
2. Repeatedly call `_consume`, to receive chunks of infotons. When you call `_consume`, you pass the position identifier and you receive and new one in the response. Pass the new position identifier to the next call to `_consume`. The process ends when CM-Well returns a 204 status code (no more content).

>**Note:** If during the streaming process a chunk is encountered that contains some invalid data, that chunk arrives with a 206 (Partial Content) result code. You can still continue to consume chunks after receiving this error.

The **consume** API does not support a guaranteed chunk length (number of infotons in a chunk). About 100 results are returned in each chunk.

>**Notes:** 
>* You must use the -v (verbose) flag, as you'll need to retrieve the new position identifier from the **X-CM-WELL-POSITION** response header.
>* The position identifier enables you to restart consuming from the same position even if the consume process fails in the middle.

## Syntax ##

**URL:** \<cm-well-host\>/_consume
**REST verb:** GET
**Mandatory parameters:** position=\<position identifier\>

----------

**Template:**

    <cm-well-host>/_consume?position=<position identifier>

**URL example:** N/A

**Curl example (REST API):**

    curl -vX GET <cm-well-host>/_consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
position | Defines the position of the chunk in the stream |  Position ID returned by create-consumer or last call to _consume | position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa

## Code Example ##

### Call ###

    curl -vX GET <cm-well-host>/_consume?position=eQA5eJwzNDE1sDQ3NDI0tjQ2rjGoMTE0MDI1NjS0BIEa_dSKxNyCnFS9_KJ0fc-8lMyyzJTSxJzimpoa&format=json

### Results ###

The following results are truncated.

    < HTTP/1.1 200 OK
    < X-CMWELL-BG: O
    < X-CMWELL-RT: 546ms
    < X-CM-WELL-N-LEFT: 162130
    < X-CMWELL-Version: 1.5.51
    < X-CMWELL-Hostname: 127.0.0.1.int.data.com
    < X-CM-WELL-POSITION: UACDeJwzNDEzMrYwMjc1NzS3qNFPSSxJ1CvJyM8tzs8rSi0tSS0q1kvOz60pqSxI1VPJqfIzdHO0ssooKSmw0ocoLk7OSM1NLNZLy8xLzEvOTMzBol0_N7UkEaRa38jAwFIXhAz13VPz04sSCzIqAYuQLXc
    < Content-Length: 149580
    < Content-Type: application/json-seq;charset=UTF8
    < Date: Thu, 19 Oct 2017 23:01:07 GMT
    <
    {
    "type": "ObjectInfoton",
    "system": {
    "uuid": "814e6f06dbff4007e0013dfb95e3b41b",
    "lastModified": "2016-05-04T17:25:23.522Z",
    "path": "\/data.com\/1-1001301743",
    "dataCenter": "dc1",
    "indexTime": 1462382724112,
    "parent": "\/data.com"
    },
    "fields": {
    "pred-IsChildObjectOf.ont": [
      "http:\/\/data.com\/1-1001296749"
    ],
    "geographyId.metadata": [
      "http:\/\/data.com\/1-1001301743"
    ],
    "geographyUniqueName.metadata": [
      "Oceania (Analytics Content)"
    ],
    "geographyName.metadata": [
      "http:\/\/data.com\/2-af3e93de80b26a50c5374d03811bef87724a79559481c8ec9db6cd209f541d38"
    ],
    "pred-IsChildObjectOf-Relationship.ont": [
      "http:\/\/data.com\/2-cba4655cc54a343e77565033c27b9466b81dc55a25b19974a85103353f73e4e8"
    ],
    "geographyType.metadata": [
      "http:\/\/data.com\/2-ca7291187cf20c0103cb2b1852f7b44317f19cd37466767c6a4bf95eac10c745"
    ],
    "type.rdf": [
      "http:\/\/data.schemas.financial.com\/metadata\/2009-09-01\/Geography"
    ],
    "permId.ont": [
      "1001301743"
    ],
    "activeFrom.Common": [
      "1900-01-01T00:00:00"
    ]
    }
      }
    
      {
    "type": "ObjectInfoton",
    "system": {
    "uuid": "580345c3a713bd8daa6ab116be85e72b",
    "lastModified": "2016-05-04T17:25:23.522Z",
    "path": "\/data.com\/1-1001301739",
    "dataCenter": "dc1",
    "indexTime": 1462382724113,
    "parent": "\/data.com"
    },
    "fields": {
    "pred-IsChildObjectOf.ont": [
      "http:\/\/data.com\/1-1001296749"
    ],
    "geographyId.metadata": [
      "http:\/\/data.com\/1-1001301739"
    ],
    "geographyUniqueName.metadata": [
      "Africa (Analytics Content)"
    ],
    "geographyName.metadata": [
      "http:\/\/data.com\/2-55783c09e9ac4758f8a95a6fb8c2f452c6b05f105d661e9040a202c7666fafa6"
    ],
    "pred-IsChildObjectOf-Relationship.ont": [
      "http:\/\/data.com\/2-ff399495b74cc2654077fa5790533f1f7714f8b18deb382e5487fcbded3edf3f"
    ],
    "geographyType.metadata": [
      "http:\/\/data.com\/2-1a6770a2f210b212cef7e0ff0622d549d2af009bbb429de656d918fcfa83476b"
    ],
    "type.rdf": [
      "http:\/\/data.schemas.financial.com\/metadata\/2009-09-01\/Geography"
    ],
    "permId.ont": [
      "1001301739"
    ],
    "activeFrom.Common": [
      "1900-01-01T00:00:00"
    ]
    }
      }
    
      {
    "type": "ObjectInfoton",
    "system": {
    "uuid": "34f40000d92156379dcb1d5600d6a08c",
    "lastModified": "2016-05-04T17:25:56.327Z",
    "path": "\/data.com\/1-110213",
    "dataCenter": "dc1",
    "indexTime": 1462382757178,
    "parent": "\/data.com"
    },
    "fields": {
    "pred-GeospatialUnitIn-Relationship.ont": [
      "http:\/\/data.com\/2-b5f997278753cae99a00cddf269a4395f4ea9970cec5e21a63e8448b063c6d63"
    ],
    "pred-NgaGnsUfi.ont": [
      "http:\/\/data.com\/2-a651fe2f6ca1871268ec2bf7e79cfad899a54ec752388609a7ed2dc4b01a93d8"
    ],
    "geographyId.metadata": [
      "http:\/\/data.com\/1-110213"
    ],
    "pred-OACityCode.ont": [
      "http:\/\/data.com\/2-b5e578a2543d975b40d85eb98a1c5f419de4fbcbca5e13a06596630dc1af2594"
    ],
    "pred-SinotrustCityCode.ont": [
      "http:\/\/data.com\/2-252cd8a7d3ab08ee230f6bacf522ed29042c265b1815dfc82a64e8a8e5702313",
      "http:\/\/data.com\/2-654acbc1c245d0ba039db31824d086def17e058224d14800c843f5625c0a8163",
      "http:\/\/data.com\/2-37e1bfb89b6c947233fb22f93fed18f2a4a99a2e2efc5a917266dabeb9606421",
      "http:\/\/data.com\/2-0b6fe6f4312c087639d5ca177c802463fd17ec0aebbed1d3d6526379b06adcb6",
      "http:\/\/data.com\/2-618a6979e12c2e2cfa4d17cd0329d3936949250c45830cf8dfe8a4208bdc88c8"
    ],
    "geographyLatitude.metadata": [
      23.6651
    ],
    "geographyLongitude.metadata": [
      116.638
    ],
    "pred-DunAndBradstreetCityCode.ont": [
      "http:\/\/data.com\/2-7cb6179e8edab68ca66e5f612fc2c153ee7f61ee6d576bc0aa0ff99e94bc8851"
    ],
    "geographyUniqueName.metadata": [
      "Chaozhou (city)"
    ],
    "geographyName.metadata": [
      "http:\/\/data.com\/2-84820596124da9e8ad6f6bf0ee446317b0738c7e1630365b619ffe8f92fb4e86",
      "http:\/\/data.com\/2-cf6c348523a6441ce6c1df6d43e37c22d308744977ed182fc70f19bc0057a898",
      "http:\/\/data.com\/2-6751587f830b54c9d4a50351d7f6140dbaf35c08f05b517dd4a1396a1ec2832b",
      "http:\/\/data.com\/2-0bc480e82026c07da99eb5d67e7a19077e4cfb8cc7853f11d4748e79d9e0fb65"
    ],
    "geographyType.metadata": [
      "http:\/\/data.com\/2-27db6ae4e64cd18655c66c36683708e7f23def3afda2c859cfd82d931e0a2585"
    ],
    "type.rdf": [
      "http:\/\/data.schemas.financial.com\/metadata\/2009-09-01\/Geography"
    ],
    "permId.ont": [
      "110213"
    ],
    "pred-GeospatialUnitIn.ont": [
      "http:\/\/data.com\/1-101175"
    ],
    "activeFrom.Common": [
      "1900-01-01T00:00:00"
    ]
      }
    }
      ... TRUNCATED
    
## Notes ##

For a faster implementation of recoverable streaming, see [bulk consume](API.Stream.ConsumeNextBulk.md).

## Related Topics ##
[Create Iterator](API.Stream.CreateIterator.md)
[Create Consumer](API.Stream.CreateConsumer.md)
[Consume Next Chunk](API.Stream.ConsumeNextChunk.md)
[Consume Next Bulk](API.Stream.ConsumeNextBulk.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.CreateConsumer.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Subscribe.SubscribeForPushedData.md)  

----