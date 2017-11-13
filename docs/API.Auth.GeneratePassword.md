# Function: *Generate Password* #

## Description ##

When creating a new CM-Well user, you can use the **generate-password** API to generate a random password and its basic-encoded value. This password can be changed later if required (see [Change Password API](API.Auth.ChangePassword.md)).

>**Note:** You will need an access token with admin permissions to call this API.

## Syntax ##

**URL:** \<CMWellHost\>/_auth?op=generate-password -H "X-CM-Well-Token:\<AdminToken\>"
**REST verb:** GET
**Mandatory parameters:** Admin-permissions token in the **X-CM-Well-Token** header

## Code Example ##

### Call ###

    curl <cm-well-host>/_auth?op=generate-password -H "X-CM-Well-Token:<AdminToken>"

### Results ###

    {"password":"t0OlrZGEM9","encrypted":"$2a$10$7AnXsjks.IZXTbpRiAGN4OQItwiz4sgxM49lvTiCjWgOhbbOQkg2m"}

## Related Topics ##
[CM-Well Security Features](DevGuide.CM-WellSecurityFeatures.md)
[Login API](API.Login.Login.md)
[Managing CM-Well Users](DevGuide.ManagingUsers.md)
[Change Password API](API.Auth.ChangePassword.md)
[Invalidate Cache API](API.Auth.InvalidateCache.md)