# Subscribing to Real-Time Updates #

You may want to subscribe to real-time updates of CM-Well, so that your application always has the latest data available. For example, if your application shows Organization data, you may want to ensure that you have the latest information before displaying an organization's details.

CM-Well supports two types of real-time subscriptions:

* **Push** - Your application provides an address to which CM-Well will post all new infotons under the path you specify.
* **Pull** - Your application requests new data under a specific path. The call only returns when there is new data available.

The following sections describe how to subscribe to push and pull real-time updates. 

## Subscribing for Pushed Data ##

Here is a template for how to subscribe to pushed data. Items in < > brackets are placeholders for actual values.

    curl -X GET '<cm-well-host>/<cm-well-path>?op=subscribe&method=push&format=<outputFormat>&qp=<fieldConditions>&callback=<http://some.host/some/path>'
    
These are the elements of the request:

Element | Description 
:--------|:------------
\<cm-well-host\>/\<cm-well-path\> | The CM-Well path whose infotons you want to receive.
op=subscribe | Indicates a "subscribe" operation.
method=push | Indicates the "push" method of receiving data.
format=\<outputFormat\> | Specifies the push output format.
qp=\<fieldConditions\> | An optional filter on the infotons you want to receive. Only those that match the field conditions are pushed.
callback=http://some.host/some/path | The address to which you want the data to be pushed.

This request returns the subscription ID that you'll need in order to unsubscribe from this search.

## Subscribing for Pulled Data ##

Here is a template for how to subscribe to pulled data. Items in < > brackets are placeholders for actual values.

    curl -X GET '<cm-well-host>/<cm-well-path>?op=subscribe&method=pull&format=<outputFormat>&qp=<fieldConditions>&bulk-size=<nofInfotons>'

These are the elements of the request:

Element | Description 
:--------|:------------
\<cm-well-host\>/\<cm-well-path\> | The CM-Well path whose infotons you want to receive.
op=subscribe | Indicates a "subscribe" operation.
method=pull | Indicates the "pull" method of receiving data.
format=\<outputFormat\> | Specifies the pull output format.
qp=\<fieldConditions\> | An optional filter on the infotons you want to receive. Only those that match the field conditions are pulled.
bulk-size=<nofInfotons> | The number of infotons that you want to receive for your next pull action.

This request blocks until the number of new infotons with the specified path and filter are available. When they are, the call returns a subscription id. You use this id to get the new infotons, as follows:

    curl -X GET '<cm-well-host>/<cm-well-path>?op=pull&sub=<subscription key>

## Unsubscribing from Real-Time Updates ##

When you no longer want to receive real-time updates, unsubscribe from updates using the ID you received from the **subscribe** call, as follows:

    curl -X GET <cm-well-host>/<cm-well-path>?op=unsubscribe&sub=<subscriptionID>

The call to **unsubscribe** is the same for both push and pull subscriptions.

    
