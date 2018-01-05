# Using Conditional Updates #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.UsingTheBlockingFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Traversal.TOC.md)  

----

Sometimes several clients can request updates to the same infoton at the same time. This can result in one client's updates overwriting another client's updates.

If you want to make sure that an infoton has not changed between the time that you retrieved its UUID and data and the time that your updates are made, you can use the **conditional update** feature. To use this feature, when requesting an update, you provide the infoton's UUID within the request. CM-Well only performs the update if the infoton's UUID is the same as the one you provided. If it's different, this indicates that another client has updated the infoton since the last time you read it.

To provide the infoton's UUID, you add a triple with the following format to your update request:

    <Infoton URI> <cmwell://meta/sys#prevUUID> <Infoton UUID>

For example:

**Request:**

    <cm-well-host>/_in?format=ntriples

**Input triples:**

    <http://infoton.int.com/example/1> <cmwell://meta/sys#prevUUId> "afb215df2b6356d8801aq10a6c764d3a" .
    <http://infoton.int.com/example/1> <http://infoton.int.com/object/name> "My New Name" .


>**Note:** An update request using the conditional feature receives a "success" response as long as the request is valid.
To find out whether the update succeeded, you'll have to use the [blocking flag](API.UsingTheBlockingFlag.md) or the [tracking API](API.Update.TrackUpdates.md). If the update failed because of a parallel update, a blocking update request or tracking request will produce an **Evicted Version** status.

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.UsingTheBlockingFlag.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Traversal.TOC.md)  

----