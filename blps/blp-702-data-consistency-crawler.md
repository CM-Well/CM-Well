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

Starting offset: min(o1,o2) if o2 is the in last offset of indexTopic ignore it (e.g. if the lag is 0).
start time: max(now1,now2)+safety net


An idea how to check duplicates without searching ES:
1. Let's assume the system is ok (either manually fixed or the DCC crawled it from the beginning of time).
2. Let's "relax" our expectations and "trust" BG to not ruin older versions.
3. for each new command:

    3.1. if it's a normal command:
    
    3.1.1 read path table. If it's the most recent one - check that it's current (in ES)
    
    3.1.2. else check that it is not current
   
   3.2. if it's an override command check from this lastModified to the most recent one. Actually from one before this lastModified.
The assumption is that dc-sync will sync most of the times the most recent one, hence, the performance penalty is not so bad.

3.1.1. read path table. If it's the most recent one - check that it's current (in ES). also check the previous lasModified that it's not current.



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
As first phase only detection will be implemented.
