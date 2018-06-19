Data Consistency Crawler (DCC)
=======================

Abstract
--------
A new component which will check the correctness of the data written to CM-Well by BG.
The verification will rely on the Kafka queue written by the web service. The goal is to detect and fix data inconsistencies proactively.

Design
------

### Container
A CM-Well cluster with N nodes has N Kafka Partitions, and N BgActors. Each BgActor, besides running IMP and Indexer streams, will also run a DCC stream.
DCC stream will benefit from same recovery mechanism BgActors are using (i.e. exactly one BgActor per Partition in the entire cluster, at any given time).

### Internal State
DCC will register itself as a Kafka Consumer and, in case of a crash (or a cluster upgrade), will resume from its last offset according to Kafka services per ClientID.
The DCC Stream will have a known finite amount of messages being currently proccessed. Hence, it can resume after a crash to offset: `last consumed offset - DCCStreamCapacity`.
The assumption is DCC can verify the same Kafka messages more than once with no harm done. 

### Verification Algorithm

#### 1) Starting point

(Crawler should never work on commands that are being processed by bg, but crawl a little bit behind it.)

This is how it's done: Crawler's starting offset is the minumum of imp.offset and the imp.offset reported by Indexer. In the same manner, the starting timestamp is the **maximum** of those two offsets' timestamps. We added a configurable amout of time as a safety net, defaults to 1 minute.

#### 2) Detection Steps

(Crawler should never Search Elasticsearch. Neither should fetch entire history from Cassndra.)

1. Consume a message from Kafka's "persist_topic" topic, with the offset according to the logic above (1).
2. According to lastModified and path, fetch from Cassandra the relevant version **and the one before that**.
3. Make sure the PATH table has no inconsistencies, and if we suspect so, consult zStore for Null Updates.
4. Fetch System Fields of those two (or one in case this is the first write to that path) versions and validate their correctness (no missing, nor duplicated system fields).
5. Get from Elasticsearch according to UUID and indexName (one or two versions) and make sure they exist, and
6. Last one should be "current" and previous one, should be current=false.


Known Limitations / Assumptions
-----------------
0. The assumption is that the web service will never return 200 OK to the user without successfully persisting the message(s) in the persist Kafka queue
0. Content is not being verified, only the existence of versions
0. Logic functions of BG (merge, null update decision etc.) works properly
0. BG always takes the correct last version, hence the merge in BG is done right
0. BG report of null update is correct
0. BG report of grouped Kafka commands on the same path is correct
0. There won't be a check to verify that a uuid is in ES and not in Cassandra.  
Argument: The check will be _for each Kafka Command_ verifying that a version exists in the paths table. Cassandra won't "miss" any infoton update so it seems that this check is not required.

Future Work
-----------
- As first phase only detection will be implemented.
- Next is the Fix phase. This bloop will be updated with the detailed design of Fix logic soon.