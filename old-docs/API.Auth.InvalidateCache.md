# Function: *Invalidate Cache* #
----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Auth.ChangePassword.md)

----

## Description ##

CM-Well manages a cache with authorization information for better performance. If you've made a change to authorization information such as creating a new user, creating a new role or changing a password, and you want the change to take immediate effect, you will need to refresh this cache. You do this by calling the **invalidate-cache** API.

>**Notes:** 
>* You will need an access token with admin permissions to call this API.
>* If you have several CM-Well nodes, then in order to refresh the caches on all nodes, you'll need to send a separate **invalidate-cache** command to each node in your cluster.

## Syntax ##

**URL:** \<CMWellHost\>/_auth?op=invalidate-cache -H "X-CM-Well-Token:\<AdminToken\>"
**REST verb:** GET
**Mandatory parameters:** Admin-permissions token in the **X-CM-Well-Token** header

## Code Example ##

### Call ###

    <CMWellHost>/_auth?op=invalidate-cache -H "X-CM-Well-Token:<AdminToken>"

### Results ###

    {"success":true}

>**Note:** Occasionally you may get a failure response when attempting to refresh the cache. This might be because a cache update is in progress when you make your request. If this happens, please retry the **invalidate-cache** request. 

## Related Topics ##
[CM-Well Security Features](DevGuide.CM-WellSecurityFeatures.md)
[Login API](API.Login.Login.md)
[Managing CM-Well Users](DevGuide.ManagingUsers.md)
[Generate Password API](API.Auth.GeneratePassword.md)
[Change Password API](API.Auth.ChangePassword.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Auth.ChangePassword.md)

----
