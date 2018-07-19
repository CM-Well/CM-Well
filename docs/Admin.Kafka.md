# Admin API Function: *_kafka* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Admin.Backpressure.md)

----

## Description ##

The **_kafka** API allows you to consume a copy of a CM-Well Kafka queue, which contains messages produced by ingest requests. You may want to use this API for some task that involves monitoring, replicating or testing CM-Well's operations on ingestion requests. (For example, the **_kafka** API is used internally to support the Data Consistency Crawler feature, which allows CM-Well to test each ingested infoton for data consistency.)

CM-Well maintains and consumes 4 Kafka **topics** (named queues):

- **persist_topic** - each message represents an infoton to be written to Cassandra storage.
- **persist_topic.priority** - same as above, for a request sent with high priority.
- **index_topic** - each message represents an infoton to be indexed in Elastic Search.
- **index_topic.priority** - same as above, for a request sent with high priority.

Each request to ingest an infoton produces two messages, one that enters one of the **persist** queues, and one that enters one of the **index** queues.

Each queue can be distributed among several **partitions**. A partition is a portion of the queue maintained on a specific CM-Well node.

When you use the **_kafka** API, in the URL you refer to the specific topic name and partition number you want to consume.

## Kafka Message Formats ##

Each message returned via the **_kafka** API is in a separate line.

When you consume the messages from the 4 Kafka topics described, CM-Well automatically deserializes them into JSON-formatted strings.

>**Note**: Currently only the 4 topics mentioned above are available for consumption. In the future, you'll be able to consume a queue populated with issues detected by the [Data Consistency Crawler](Architecture.DCC.md).)

If you are consuming a different topic, and you know that its messages are also strings, you can specify the **format=text** parameter value to cause its messages to be deserialized as strings. Otherwise, each message is returned as comma-separated byte values (e.g. ```72,101,108,108,111,32,87,111,114,108,100...```).

## Syntax ##

**URL:** ```<cm-well-host>/_kafka/<topicName>/<partitionNumber>```
**REST verb:** GET
**Mandatory parameters:** None.

----------

**Template:**

    curl -X GET "<cm-well-host>/_kafka/<topicName>/<partitionNumber>" -H X-CM-Well-Token:<admin-token>

**URL example:** ```<cm-well-host>/_kafka/persist_topic/0?format=text```

**Curl example (REST API):**

    curl -X GET "<cm-well-host>/_kafka/persist_topic/0?format=text" -H X-CM-Well-Token:<admin-token>


## Parameters ##

Parameter | Description | Values 
:----------|:-------------|:--------
offset | Optional. Message offset to start from. If not provided, messages will be consumed starting from the beginning of the queue. | 0 to (queue size - 1)
max-length | Optional. The maximal number of messages to return. If not provided, consumption will continue until the end of the queue. | 1 to queue size
format | Optional. The only valid value is **text**; i.e. the parameter must appear as **format=text**. In this case, each message will be converted into a JSON-formatted string. Otherwise, the message's bytes will be returned as is. | text

## Code Example ##

### Call ###

    curl "http://<cm-well-host>/_kafka/persist_topic/0?offset=604&max-length=3" -H X-CM-Well-Token:<admin-token>

### Results ###

    WriteCommand(ObjectInfoton(/meta/auth/users,lh,None,2018-06-25T13:32:48.935Z,None,),None,None)
    WriteCommand(ObjectInfoton(/meta/logs/version-history,lh,None,2018-06-25T13:32:49.040Z,None,),None,None)
    WriteCommand(ObjectInfoton(/meta/auth,lh,None,2018-06-25T13:32:48.935Z,None,),None,None)


## Notes ##

* Using the _kafka API to access Cm-Well system queues requires an Admin token.
* When you use the **_kafka** API, you don't affect the original CM-Well system queue,  rather you consume a copy of that queue.

## Related Topics ##

None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Admin.Backpressure.md)

----
