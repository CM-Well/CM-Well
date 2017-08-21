# Function: *Pull New Data* #

## Description ##
You may want to subscribe to real-time updates of CM-Well, so that your application always has the latest data available. For example, if your application shows Organization data, you may want to ensure that you have the latest information before displaying an organization's details.

CM-Well supports two types of real-time subscriptions:

* **Push** - Your application provides an address to which CM-Well will post all new infotons that were created under the path you specified (see [Subscribe for Pushed Data](API.Subscribe.SubscribeForPushedData.md)).
* **Pull** - Your application requests new data created under a specific path. You make one call to subscribe to updates, which returns a token. You then make a call to pull the updates (see []()). The **pull** call only returns when there is new data available. You can make repeated calls to **pull**, to obtain the latest updates.

When you no longer need the real-time updates, terminate the subscription (by calling [unsubscribe]()), while passing the token you received from the **subscribe** call.

## Syntax ##

**URL:** \<cm-well-host\>/\<cm-well-path\>
**REST verb:** GET
**Mandatory parameters:** op=pull&sub=<token>

----------

**Template:**

    curl "<cm-well-host>/<cm-well-path>?op=pull&sub=<token>"

**URL example:** 

    <cm-well-host>/permid.org?op=pull&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXBwZUAxMC4yMDQuNzMuMTQ0OjM5NzQwL3VzZXIvYzU3MTY1MTE

**Curl example (REST API):**

    curl "<cm-well-host>/permid.org?op=pull&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXBwZUAxMC4yMDQuNzMuMTQ0OjM5NzQwL3VzZXIvYzU3MTY1MTE"

## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example 
:----------|:-------------|:--------|:---------|:----------
sub=\<token\> | The token you received in response to the **subscribe** call | sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXBwZUAxMC4yMDQuNzMuMTQ0OjM5NzQwL3VzZXIvYzU3MTY1MTE

## Code Example ##

### Call ###

    curl "<cm-well-host>/permid.org?op=pull&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXBwZUAxMC4yMDQuNzMuMTQ0OjM5NzQwL3VzZXIvYzU3MTY1MTE&format=text"

### Results ###

        o:nRSR4409Ea-2016-07-18
        sys:data                       "<Document>\n<Source>RNS</Source>..." ;
        sys:dataCenter                 "dc1" ;
        sys:indexTime                  "1468833593106"^^xsd:long ;
        sys:lastModified               "2016-07-18T09:19:51.344Z"^^xsd:dateTime ;
        sys:length                     "11981"^^xsd:long ;
        sys:mimeType                   "text/plain; utf-8" ;
        sys:parent                     "/data.com/sc/docs/input/news" ;
        sys:path                       "/data.com/sc/docs/input/news/nRSR4409Ea-2016-07-18" ;
        sys:type                       "FileInfoton" ;
        sys:uuid                       "05e3ae867c2928cb7cd2e8ec254bf005" ;
        supplyChain:Codes              "LSEN" ;
        supplyChain:DATE               "2016-07-18"^^xsd:dateTime ;
        supplyChain:Feed               "UCDP" ;
        supplyChain:MetaCodes.product  "LSEN" ;
        supplyChain:Source             "RNS" ;
        supplyChain:Urgency            "3"^^xsd:int .

## Notes ##

Remember to unsubscribe if you no longer need real-time updates.

## Related Topics ##
[Subscribe for Pulled Data](API.Subscribe.SubscribeForPulledData.md)
[Subscribe for Pushed Data](API.Subscribe.SubscribeForPushedData.md)
[Unsubscribe](API.Subscribe.Unsubscribe.md)

