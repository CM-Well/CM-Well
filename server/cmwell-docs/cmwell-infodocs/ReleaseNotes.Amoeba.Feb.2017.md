# CM-Well Version Release Notes - Amoeba (Feb. 2017) #

Version Name | Version Number | Release Date PPE | Release Date Production 
-------------|-----------------|----------------|---------------------------
Amoeba | 1.5.414 | Feb. 7th 2017 | Feb. 13th 2017

## Change Summary ##

### New Features ###

* [Pushback Pressure for Ingest to Prevent Slowdowns](#hdr1)
* [Kafka Partition Reassignment for Inoperative Machine](#hdr2)
* Version name added to CM-Well web UI.

### Notable Bug Fixes ###
**GitLab Item No. 321:** Fix race condition in system restart, related to Legal Analytic's use of Oracle Linux 7.1.

### Changes to API	 ###
N/A

### Link to Demo ###
N/A

------------------------------

## Feature Descriptions ##

<a name="hdr1"></a>
### Pushback Pressure for Ingest to Prevent Slowdowns ###

**GitLab Item No.:** 322.

**Description:**

We have encountered situations where a significant load of ingest requests causes CM-Well's response time to degrade severely. To prevent this situation, a "pushback pressure" feature has been added to ingestion, so that if ingestion queues are too full, new requests may get the HTTP response code 503. In this case, the recommended behavior is for the client to "sleep" for a brief period and retry the request. (Sleep for 1 second for the first retry, then if necessary increase sleep period to up to 30 seconds.)

**Documentation:** 
N/A

----------

<a name="hdr2"></a>
### Kafka Partition Reassignment for Inoperative Machine ###

**GitLab Item No.:** 176.

**Description:**
This feature enhances infrastructure stability and has no direct effect on the user. CM-Well maintains 3 copies of each data item for redundancy. If a machine goes offline for 15 minutes, it is assumed inoperative, and its data is rewritten to other machines. The "Kafka partition reassignment" feature refers to triggering replication of the Kafka queue management module's data in case of a "dead" machine.


**Documentation:** 
N/A