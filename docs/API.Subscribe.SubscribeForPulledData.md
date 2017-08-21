# Function: *Subscribe for Pulled Data* #

## Description ##
You may want to subscribe to real-time updates of CM-Well, so that your application always has the latest data available. For example, if your application shows Organization data, you may want to ensure that you have the latest information before displaying an organization's details.

CM-Well supports two types of real-time subscriptions:

* **Push** - Your application provides an address to which CM-Well will post all new infotons that were created under the path you specified (see [Subscribe for Pushed Data](API.Subscribe.SubscribeForPushedData.md)).
* **Pull** - Your application requests new data created under a specific path. You make one call to subscribe to updates, which returns a token. You then make a call to pull the updates (see [Pull New Data](API.Subscribe.PullNewData.md)). The **pull** call only returns when there is new data available.

When you no longer need the real-time updates, terminate the subscription (by calling [unsubscribe](API.Subscribe.Unsubscribe.md)), while passing the token you received from the **subscribe** call.

## Syntax ##

**URL:** \<cm-well-host\>
**REST verb:** GET
**Mandatory parameters:** op=subscribe&method=pull

----------

**Template:**

    curl -X GET "<cm-well-host>/<cm-well-path>?op=subscribe&method=pull&format=<format>&qp=<query parameters>&bulk-size=<nofInfotons>"

**URL example:** 

    <cm-well-host>/permid.org?op=subscribe&method=pull&qp=CommonName.mdaas:Intel&format=ttl&bulk-size=1

**Curl example (REST API):**

    curl -X GET "<cm-well-host>/permid.org?op=subscribe&method=pull&qp=CommonName.mdaas:Intel&format=ttl&bulk-size=1"


## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Values | Example 
:----------|:-------------|:--------|:---------
method=pull | Indicates that the subscription is in pull mode | N/A | N/A
bulk-size | The number of infotons you want to pull | Integer value >=1 | bulk-size=10 

## Code Example ##

### Call ###

    curl -X GET "<cm-well-host>/permid.org?op=subscribe&method=pull&qp=CommonName.mdaas:Intel&format=ttl&bulk-size=1"

### Results ###

The call returns a subscription token:

    YWtrYS50Y3A6Ly9jbS13ZWxsLXBwZUAxMC4yMDQuNzMuMTQ3OjU4Nzg1L3VzZXIvMWVlYzg1ZGEs

## Notes ##

Remember to unsubscribe if you no longer need real-time updates.

## Related Topics ##
[Pull New Data](API.Subscribe.PullNewData.md)
[Subscribe for Pushed Data](API.Subscribe.SubscribeForPushedData.md)
[Unsubscribe](API.Subscribe.Unsubscribe.md)

