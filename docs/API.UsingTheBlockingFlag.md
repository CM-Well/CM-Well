# Using the blocking Flag #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.UsingTheWith-deletedFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingConditionalUpdates.md)  

----

By default, CM-Well handles update requests asynchronously, that is, the caller gets a response when the request is made and not when its processing is completed. In some cases, you may prefer that your update request be handled synchronously, also known as "in blocking mode". 

To do this, you can add the **blocking** flag to your update request. In this case, the call only returns when the request has been processed.

Here is an example:

**Request:**

    curl -X POST "<cm-well-host>/_in?format=ttl&replace-mode&blocking" -H "Content-Type: text/plain" --data-binary @curlInput.txt

**Response:**

HTTP 200 code.

Response body:

    <http://permid.org/1-42965572445> <cmwell://meta/nn#trackingStatus> "Done" .
    <http://permid.org/1-42963422545> <cmwell://meta/nn#trackingStatus> "Done" .
    <http://permid.org/1-22235545445> <cmwell://meta/nn#trackingStatus> "Failed" .
    <http://permid.org/1-54296574112> <cmwell://meta/nn#trackingStatus> "Evicted(expected:8029b2df973dd216375a7c5a7761a2be,actual:c94bf0de57f83874a6bb5983bdef4b8d)" .
    <http://permid.org/1-54296574112> <cmwell://meta/nn#trackingStatus> "Done" .

The response body contains the status values for every infoton the request tried to update.
This is similar to the response that CM-Well returns when you use the [tracking API](API.Update.TrackUpdates.md). 

>**Note:** You may configure your HTTP client to time out after a certain period.
>In this case, a blocking update request may time out before it's completed, 
>and therefore we recommend using the [tracking API](API.Update.TrackUpdates.md) to test the request's status.
>In this case, if you want the update to behave synchronously, you'll have to manage 
>tracking and verifying completion in your own code.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.UsingTheWith-deletedFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.UsingConditionalUpdates.md)  

----