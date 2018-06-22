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

If you are consuming a different topic (YAAKOV, LIKE WHAT???), and you know that its messages are also strings, you can specify the **format=text** parameter value to cause its messages to be deserialized as strings. Otherwise, each message is returned as comma-separated byte values (YAAKOV, why do we need the commas?).

## Syntax ##

**URL:** ```<cm-well-host>/_kafka/<topicName>/<partitionNumber>```
**REST verb:** GET
**Mandatory parameters:** None.

----------

**Template:**

    curl -X GET "<cm-well-host>/_kafka/<topicName>/<partitionNumber>"

**URL example:** ```<cm-well-host>/_kafka/persist_topic/0?format=text```

**Curl example (REST API):**

    curl -X GET "<cm-well-host>/_kafka/persist_topic/0?format=text"


## Parameters ##

Parameter | Description | Values 
:----------|:-------------|:--------
offset | Optional. Message offset to start from. If not provided, messages will be consumed starting from the beginning of the queue. | 0 to (queue size - 1)
max-length | Optional. The maximal number of messages to return. If not provided, consumption will continue until the end of the queue and will then hang until a new message is produced. | 1 to queue size
format | Optional. The only valid value is **text**; i.e. the parameter must appear as **format=text**. In this case, each message will be converted into a JSON-formatted string. Otherwise, the message's bytes will be returned as is. | text

## Code Example ##

### Call ###

    curl "http://<cm-well-host>/_kafka/persist_topic/0?offset=604&max-length=3"

### Results ###

    YAAKOV PLEASE PROVIDE RESPONSE EXAMPLE.


## Notes ##

* Using the _kafka API to access Cm-Well system queues requires an Admin token.
* When you use the **_kafka** API, you don't affect the original CM-Well system queue,  rather you consume a copy of that queue.

## Related Topics ##

None.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Admin.Backpressure.md)

----