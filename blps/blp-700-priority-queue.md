# Priority Queue - Design and API

## Abstract
CM-Well ingest is "eventually consistent". Today, any payload that was ingested successfully, is pushed to a queue (Kafka) and the bg process handles each chunk of it in first-in first-out (FIFO) manner (as much as it can, given its distributed nature). Introducing a Priority Queue means some (more "urgent") HTTP Posts will have a special treatment, and a ticket to pass all chunks currently in line.

## API
* Write endpoints to CM-Well (`_in`, and low-level JSON POST) will accept a new query parameter: `priority` (with no value).
* A post with this query parameter will bypass all currently in-queue messages and will have a good chance (see Implementation below) to become the next chunk to be processed by bg.
* In addition to the query parameter, to avoid abuse of the system, an `X-CM-WELL-TOKEN` request header must be provided, with a value of a write-token of a user with the "Priority" role. (in case of a post with `priority` query parameter and no valid Authorization, an HTTP 403 Forbidden will be returned)
* There are no further restrictions, e.g. it can be combined with `tracking` or `blocking` query parameters.

## Implementation
* A new Kafka topic will be introduced.
* The WS will normally write to the exiting topic, unless the `priority` query parameter is present. Assuming request is authorized, WS will write to that new topic.
* Even when PBP will reject regular write (cases when queue is "full"), a priority write will make it through.
* bg will process first all messages from the priority topic, and only if empty will proceed with regular topic.
* When handling a message from the prioritized topic, bg will amend the `lastModified` of the Infoton to be the current system time, in order to avoid conflicts in Cassandra.
* This is only supported in new data path.

## Hands-On Example
Assuming the user "ManualEditor" exists in /meta/auth/users with `"opertaions":["PriorityWrite"]` within its JSON content. And `$token` is a valid token for that user.

* Trying to write with the `priority` query parameter but without a token supplied will yield 403:
```
$ curl "localhost:9000/_in?format=ntriples&priority" --data-binary '<http://example.org/Individuals/JohnSmith> <http://www.tr-lbd.com/bold#active> "778" .'
{"success":false,"message":"User not authorized for priority write"}
```

* Trying to write with the token:
```
$ curl "localhost:9000/_in?format=ntriples&priority" --data-binary '<http://example.org/Individuals/JohnSmith> <http://www.tr-lbd.com/bold#active> "778" .' -H "X-CM-WELL-TOKEN:$token"
{"success":true}
```

## Future Work
* One or a few more (up to a fixed small number) priorites can be introduced. For example "idle queue" which is intended for Admin operations, which will be prioritzed higher than normal but lower than the Priority priority.
