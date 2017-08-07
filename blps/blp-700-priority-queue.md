# Priority Queue - Design and API

## Abstract
CM-Well ingest is "eventually consisted". Today, any payload that was ingested successfully, is pushed to a queue (Kafka) and the bg process handles each chunk of it in First Come First Served manner (as much as it can, given its distributed nature). Introducing a Priority Queue means some (more "urgent") HTTP Posts will have a special treatment, and a ticket to pass all chunks currently in line.

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

## Future Work
* More levels of priority can be introduced. Theoretically, the `priority` query parameter can accept an Integer value from 0 (which is equivalent to not providing the query parameter at all) to MAX_INT. But we will merely add one more, i.e. the values of 1 or 2 can be used. 1 means medium priority and 2 means high priority.
* The change above can be reflected in Authorization as well, i.e. the Priority Role will accept a value of max priority allowed for user. This part is optional, we can decide any user with the Priority role can use any value of priority query parameter.
