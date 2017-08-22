# Function: *Unsubscribe from Real-Time Updates* #

## Description ##

If you have subscribed for real-time updates via [pushed](API.Subscribe.SubscribeForPushedData.md) or [pulled](API.Subscribe.SubscribeForPulledData.md) data, you can unsubscribe if you no longer need these updates.

The call to unsubscribe is the same, regardless of whether you have requested pushed or pulled data.

## Syntax ##

**URL:** \<CMWellHost\>
**REST verb:** GET
**Mandatory parameters:** op=unsubscribe&sub=<subscriptionToken>

----------

**Template:**

    curl -X GET <cm-well-host>/<cm-well-path>?op=unsubscribe&sub=<subscriptionToken>

**URL example:** 

    <cm-well-host>/permid.org?op=unsubscribe&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2ODo1NjA2MS91c2VyLzNkMDZjZWE1

**Curl example (REST API):**

    curl -X GET "<cm-well-host>/permid.org?op=unsubscribe&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2ODo1NjA2MS91c2VyLzNkMDZjZWE1"


## Special Parameters ##

Parameter | Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Example 
:----------|:-------------|:--------
sub | The token you received from the subscription call| sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2ODo1NjA2MS91c2VyLzNkMDZjZWE1

## Code Example ##

### Call ###

    curl -X GET "<cm-well-host>/permid.org?op=unsubscribe&sub=YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2ODo1NjA2MS91c2VyLzNkMDZjZWE1"

### Results ###

The call returns a subscription token:

    unsubscribe YWtrYS50Y3A6Ly9jbS13ZWxsLXByb2RAMTAuMjA0LjczLjE2ODo1NjA2MS91c2VyLzNkMDZjZWE1

## Notes ##
None.

## Related Topics ##
[Subscribe for Pulled Data](API.Subscribe.SubscribeForPulledData.md)
[Subscribe for Pushed Data](API.Subscribe.SubscribeForPushedData.md)


