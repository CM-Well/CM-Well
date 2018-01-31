# Function: *Generate Password* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.TrackUpdates.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Auth.ChangePassword.md)  

----

## Description ##

When creating a new CM-Well user, you can use the **generate-password** API to generate a random password and its encrypted value. This password can be changed later if required (see [Change Password API](API.Auth.ChangePassword.md)).

## Syntax ##

**URL:** \<CMWellHost\>/_auth?op=generate-password
**REST verb:** GET
**Mandatory parameters:** None.

## Code Example ##

### Call ###

    curl <cm-well-host>/_auth?op=generate-password

### Results ###

    {"password":"t0OlrZGEM9","encrypted":"$2a$10$7AnXsjks.IZXTbpRiAGN4OQItwiz4sgxM49lvTiCjWgOhbbOQkg2m"}

## Related Topics ##
[CM-Well Security Features](DevGuide.CM-WellSecurityFeatures.md)
[Login API](API.Login.Login.md)
[Managing CM-Well Users](DevGuide.ManagingUsers.md)
[Change Password API](API.Auth.ChangePassword.md)
[Invalidate Cache API](API.Auth.InvalidateCache.md)

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Update.TrackUpdates.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Auth.ChangePassword.md)  

----
