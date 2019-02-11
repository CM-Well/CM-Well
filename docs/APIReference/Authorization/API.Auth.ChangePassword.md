# Change Password

## Description

Usually new users are assigned random passwords. After creating a CM-Well user, you may want to change the user's password to a customized value. You can do this with the **change-password** function.

!!! note
	To change a user's password, you'll need an access token for the user. To obtain an access token, you can call the [_login API](API.Auth.Login.md).

## Syntax

**URL:** ```<CMWellHost>/_auth?op=change-password&current=87654321&new=12345678 -H "X-CM-Well-Token:<AccessToken>"```

**REST verb:** GET

**Mandatory parameters:** Access token in the **X-CM-Well-Token** header

## Special Parameters

Parameter | Description 
:---------|:-------------
current   | The user's current password
new       | The user's new password

## Code Example

### Call

```
    <CMWellHost>/_auth?op=change-password&current=$2a$10$7AnXsjks.IZXTbpRiAGN4OQItwiz4sgxM49lvTiCjWgOhbbOQkg2m&new=12345678 -H "X-CM-Well-Token:<AccessToken>"
```

### Results

```
    {"success":true}
```

## Related Topics

[CM-Well Security Features](../../DeveloperGuide/DevGuide.CM-WellSecurityFeatures.md)
[Login API](API.Auth.Login.md)

[Managing CM-Well Users](../../DeveloperGuide/DevGuide.ManagingUsers.md)

[Generate Password API](API.Auth.GeneratePassword.md)

[Invalidate Cache API](API.Auth.InvalidateCache.md)


