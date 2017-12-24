# Function: *Track Updates* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.AddLinkInfoton.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Auth.GeneratePassword.md)  

----

## Description ##

By default, when you send an update request to CM-Well, it queues the request for processing and returns immediately, providing a response code that indicates the validity of the request. There are two ways to determine the processing status of the request:

1. Use the [blocking flag](API.UsingTheBlockingFlag.md) to cause the update request to be synchronous.
2. Use the **tracking** flag to receive a tracking ID for the request, and poll for the processing status using this ID.

Here is an example of a call that uses the **tracking** flag:

    <cm-well-host>/_in?format=ntriples&tracking ...

If the request is valid, it returns an HTTP response code of 200 and a **X-CM-WELL-TRACKING** header, whose value is a unique ID for the request. You can use the tracking ID to call the **_track** API and poll for the status of your update request.
The response to a **_track** call contains status values for each infoton you're attempting to update.

The following table describes the status values you can receive for an updated infoton:

Value | Description
:------|:-------------
InProgress | The request is still being processed.
Done | The request was completed successfully.
Failed | The request failed.
Evicted(*\<previous and current UUIDs\>*) | You requested a [conditional update](API.UsingConditionalUpdates.md), which was denied due to a conflict.

## Syntax ##

**URL:** <CMWellHost>/_track/<Tracking ID>
**REST verb:** GET
**Mandatory parameters:** <Tracking ID>

----------

**Template:**

    curl -X GET <cm-well-host>/_track/<Tracking ID>

**URL example:** <cm-well-host>/_track/SGVsbG8gV29ybGQ

**Curl example (REST API):**

    curl -X GET "<cm-well-host>/_track/SGVsbG8gV29ybGQ"

## Code Example ##

### Call ###

    curl -X GET "<cm-well-host>/_track/SGVsbG8gV29ybGQ"

### Results ###

    <http://permid.org/1-42965572445> <cmwell://meta/nn#trackingStatus> "Done" .
    <http://permid.org/1-42963422545> <cmwell://meta/nn#trackingStatus> "In Progress" .
    <http://permid.org/1-22235545445> <cmwell://meta/nn#trackingStatus> "Failed" .
    <http://permid.org/1-54296574112> <cmwell://meta/nn#trackingStatus> "Evicted(expected:8029b2df973dd216375a7c5a7761a2be,actual:c94bf0de57f83874a6bb5983bdef4b8d)" .
    <http://permid.org/1-54296574112> <cmwell://meta/nn#trackingStatus> "Done" .

## Notes ##

* If the request produces any kind of error that prevents it from being processed (for example, 40x or 50x errors) the **X-CM-WELL-TRACKING** header is not returned.
* Tracking IDs are valid for 15 minutes after the related request was completed. If you call the _track API with an invalid or obsolete ID, you will get the HTTP 410 (Gone) error.

## Related Topics ##
[Using the blocking Flag](API.UsingTheBlockingFlag.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.AddLinkInfoton.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Auth.GeneratePassword.md)  

----