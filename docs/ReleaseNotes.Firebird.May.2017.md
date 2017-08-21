# CM-Well Version Release Notes - Firebird (May 2017) #

## Change Summary ##

### New Features ###

* [Conditional Updates to Prevent Write Conflicts](#hdr1)
* [Track Update Progress](#hdr2)
* [Synchronous Update](#hdr4)

### Notable Bug Fixes ###
[Add Node to CM-Well Cluster](#hdr4)

### Changes to API	 ###
* New **_track** endpoint for tracking update requests.
* New **tracking** flag to produce a tracking ID for update requests.
* New **blocking** flag to cause update requests to be blocking (synchronous).
* New option to add **prevUUID** triple, to indicate conditional update.

### Link to Demo ###
N/A

------------------------------

## Feature Descriptions ##

<a name="hdr1"></a>
### Conditional Updates to Prevent Write Conflicts ###

**GitLab Item No.:** 366.

**Description:**
This feature allows you to prevent a situation where several clients' parallel update requests for the same infoton can cause overwriting of data.

**Documentation:** 
[Conditional updates](API.UsingConditionalUpdates.md)

----------

<a name="hdr2"></a>
### Track Update Progress ###

**GitLab Item No.:** 371.

**Description:**
This feature allows you to track the status of an update request, using the new **_track** endpoint.

**Documentation:** 
[Tracking API](API.Update.TrackUpdates.md)

----------

<a name="hdr3"></a>
### Synchronous Update ###

**GitLab Item No.:** 365.

**Description:**
By adding the new **blocking** flag to an update request, you can cause the request to be blocking (synchronous) instead of the default asynchronous behavior.

**Documentation:** 
[Blocking flag](API.UsingTheBlockingFlag.md)

----------

<a name="hdr4"></a>
### Bug Fix: Add Node to CM-Well Cluster ###

**GitLab Item No.:** 358.

**Description:**
Previously there was a bug that caused the "add node" admin script to fail; now fixed.

**Documentation:** 
N/A