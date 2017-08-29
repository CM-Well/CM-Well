# Function: *Get Next Chunk* #

## Description ##

If you wish to retrieve a large number of infotons, but you want to iterate over small "chunks" of data in a controlled fashion, you can use the **create-iterator** and **next-chunk** APIs. This allows you to request the number of infotons you want to process, and receive only that number during each iteration.

The process requires two different API calls:
1. Call **create-iterator** to receive an iterator ID (in the **iteratorId** field) for the query.
2. Repeatedly call **next-chunk**, specifying a **length** value, to receive that number of infotons. When you call **next-chunk**, you pass the iterator ID you received when you called **create-iterator**. The process ends when CM-Well returns an empty list.

## Syntax ##

**URL:** \<cm-well-host\>/\<cm-well-path\>
**REST verb:** GET
**Mandatory parameters:** op=next-chunk

----------

**Template:**

    <cm-well-host>/<cm-well-path>?op=next-chunk&length=<chunk size>&format=<format>&iterator-id=<iteratorId>

**URL example:**
   `<cm-well-host>/permid.org?op=next-chunk&length=50&format=json&iterator-id=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny43OjUzMDY3L3VzZXIvJENscCM4MjY5NjY1NzQ`

**Curl example (REST API):**

    Curl -X GET <cm-well-host>/permid.org?op=next-chunk&length=50&format=json&iterator-id=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny43OjUzMDY3L3VzZXIvJENscCM4MjY5NjY1NzQ

> **Note:** An alternate syntax for retrieving the next chunk is:
> 
> **Template:** `<cm-well-host>/_iterate?iterator-id=<token>`
> 
> **Example:**  `curl "<cm-well-host>/_iterate?iterator-id=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2MTo0MDEzMi91c2VyLyRHfmViIy04MDc5OTg5OTQ"`
    
## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example
:----------|:-------------|:--------|:---------
session-ttl | The time, in milliseconds, until the iteration session expires. The iteration token is only valid for this length of time. The default value is 15 seconds; the maximal value is 60 seconds. | A positive integer up to 60000. | session-ttl=20000 (20 seconds)

## Code Example ##

### Call ###

    curl <cm-well-host>/permid.org?op=next-chunk&iterator-id=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny43OjUzMDY3L3VzZXIvJENscCM4MjY5NjY1NzQ&length=3&format=json

### Results ###
    {
      "type": "IterationResults",
      "iteratorId": "YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjE3Ny41OjQxNDAwL3VzZXIvJGdicCMxMjI5ODU0NDI2",
      "totalHits": 91053224,
      "infotons": [
        {
            "type": "ObjectInfoton",
            "system": {
                "uuid": "b55c0bcc1b97d5488cef8619b7bde9cf",
                "lastModified": "2015-03-08T12:15:03.285Z",
                "path": "\/permid.org\/1-30064778964",
                "dataCenter": "dc1",
                "parent": "\/permid.org"
             }
        },
        {
           "type": "ObjectInfoton",
           "system": {
                "uuid": "f872f4986fd9f3aa5db39b6b492da7cf",
                "lastModified": "2015-04-01T02:05:08.331Z",
                "path": "\/permid.org\/1-21557484859",
                "dataCenter": "dc1",
                "parent": "\/permid.org"
           }
        },
        {
           "type": "ObjectInfoton",
           "system": {
                "uuid": "b5d00287d5de49bfdc583849416f7659",
                "lastModified": "2015-06-02T22:30:37.385Z",
                "path": "\/permid.org\/1-5045869909",
                "dataCenter": "dc1",
                "parent": "\/permid.org"
           }
        }
      ]
    }

## Notes ##

* If the iteration process fails in the middle for any reason, you will have to restart the process from the beginning (that is, iterate again over all infotons that match the query).
* An alternative is to use the **consumer** API (see **Related Topics**), which allows you to save the iteration state and restart from the same point after a failure.

## Related Topics ##
[Create Iterator](API.Stream.CreateIterator.md)
[Create Consumer](API.Stream.CreateConsumer.md)
[Consume Next Chunk](API.Stream.ConsumeNextChunk.md)


