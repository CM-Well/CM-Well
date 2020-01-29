# Function: *Subscribe for Pushed Data* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.ConsumeNextChunk.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Subscribe.SubscribeForPulledData.md)  

----

## Description ##
You may want to subscribe to real-time updates of CM-Well, so that your application always has the latest data available. For example, if your application shows Organization data, you may want to ensure that you have the latest information before displaying an organization's details.

CM-Well supports two types of real-time subscriptions:

* **Push** - Your application provides an address to which CM-Well will post all new infotons that were created under the path you specified.
* **Pull** - Your application requests new data created under a specific path. The call only returns when there is new data available (see [Subscribe for Pulled Data](API.Subscribe.SubscribeForPulledData.md)).

The subscribe call returns a token that you use when terminating the subscription (calling [unsubscribe](API.Subscribe.Unsubscribe.md)).

## Syntax ##

**URL:** \<cm-well-host\>
**REST verb:** GET
**Mandatory parameters:** op=subscribe&method=push

----------

**Template:**

    curl -X GET <cm-well-host>/<cm-well-path>?op=subscribe&method=push&qp=<query parameters>&format=<format>&callback=<callbackURL>

**URL example:** 

    <cm-well-host>/permid.org?op=subscribe&method=push&qp=CommonName.mdaas=Intel&format=ttl&callback=http://mycallback/path

**Curl example (REST API):**

    curl -X GET <cm-well-host>/permid.org?op=subscribe&method=push&qp=CommonName.mdaas=Intel&format=ttl&callback=http://mycallback/path

## Special Parameters ##

Parameter | Description
:----------|:-------------
method=push | Indicates that the subscription is in push mode

## Code Example ##

### Call ###

    curl -X GET <cm-well-host>/permid.org?op=subscribe&method=push&qp=CommonName.mdaas=Intel&format=ttl&callback=http://mycallback/path

### Results ###

The call returns a subscription token:

    YWtrYS50Y3A6Ly9jbS13ZWxsLXBwZUAxMC4yMDQuNzMuMTQ3OjU4Nzg1L3VzZXIvMWVlYzg1ZGEs

## Notes ##

Remember to unsubscribe if you no longer need real-time updates.

## Related Topics ##
[Subscribe for Pulled Data](API.Subscribe.SubscribeForPulledData.md)
[Unsubscribe](API.Subscribe.Unsubscribe.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Stream.ConsumeNextChunk.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Subscribe.SubscribeForPulledData.md)  

----
