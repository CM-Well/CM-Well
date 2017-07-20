# CM-Well Security Features #

CM-Well protects write-access to production environments by requiring an authentication token for write operations. (Write operations to non-production environments and read operations from any environment currently do not require a token, but this is subject to change.)

> **Notes:**
> * CM-Well currently does not support internal encryption of its data. If necessary, you can encrypt field values in your application, before uploading them to CM-Well.


The following sections describe how to obtain and use the CM-Well authentication token.

## Users, Passwords and Permissions ##

A CM-Well user does not represent a person, but rather an application that calls CM-Well. You use a username and password when logging into CM-Well, and you receive a corresponding authentication token to pass to subsequent API calls.

Each user has specific read and write permissions. A user may have access to certain paths in CM-Well and not to others. You may want to define more than one user for your application, if end users with different profiles may be using it, who require different access permissions.

>**Note:** If you are running a privately compiled version of CM-Well, the default root user name and password are "root" and "root".

## Tokens ##

When you log into CM-Well, in the response you receive an authentication token that looks like this:
 
`eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteVVzZXJuYW1lIiwiZXhwIjoxNDY2NTg5NzYzMDMzLCJyZXYiOjB9.17s3USkZaZYbifXsgmm8eaEmr66SP6udbuUQCLwcWKY`

(The token in is [JWT format](http://jwt.io).)

You pass this token to each subsequent call to CM-Well, as the value of the **X-CM-WELL-TOKEN** HTTP header. This enables the access permissions assigned to your user.

By default, tokens are valid for 24 hours after they are created. You can call `_login` again to receive a new token when the previous one expires. You can also request a custom expiration period when logging in (see below).


## The _login API ##

CM-Well provides a **login** API call, which allows you to log into CM-Well with the username/password and receive a corresponding authentication token to pass to subsequent API calls.

See [Login API](API.Login.Login.md) to learn more.








