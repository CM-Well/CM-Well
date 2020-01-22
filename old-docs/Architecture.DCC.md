# Architecture: Data Consistency Crawler (DCC) #

## Description ##

The Data Consistency Crawler (DCC) is an internal CM-Well module. The goal of the DCC is to proactively test for inconsistent data in the infotons written to CM-Well, and fix inconsistencies.

>**Note**: The current version of the DCC only tests for and logs data inconsistencies. Future versions will fix them as well.

## Design ##

The DCC is a module that runs within the CM-Well Background (BG) process, and is transparent to CM-Well users. It uses the **_kafka** API to consume a copy of the Kafka **persist_topic** queue (the queue filled with ingest messages by the Web Service process). Infotons are reviewed shortly after being processed (ingested) by BG. DCC tests each infoton in the queue for data inconsistencies. 

>**Note**: DCC's effect on performance is negligible.

A CM-Well cluster with N nodes has N Kafka Partitions, and N BgActors. Each BgActor, besides running Infoton Merge Process (IMP) and Indexer streams,  also runs a DCC stream. If DCC operation is interrupted for any reason, the DCC resumes from its last known offset, which is persisted in **zStore**. The assumption is that DCC can verify the same Kafka messages more than once with no harm done (its operation is idempotent).

## Consistency Tests ##

This is the DCC's logic when testing for data inconsistencies:

1. Consume a message from Kafka's **persist_topic** topic.
1. According to the infoton's last modified time and path, fetch from Cassandra the relevant version and the one before that.
1. Make sure the PATH table has no inconsistencies, and if we suspect it does, consult zStore for Null Updates.
1. Fetch the system fields of those two infoton versions (or one version, if this is the first write to that path) and validate their correctness (no missing or duplicated system fields).
1. Query Elastic Search according to UUID and indexName (one or two versions) and make sure:
    - The fields exist.
    - The last version written is current.
    - The previous version written isn't current.

## More Information ##

For more information, see [DCC Design](https://github.com/thomsonreuters/CM-Well/blob/master/blps/blp-702-data-consistency-crawler.md).

