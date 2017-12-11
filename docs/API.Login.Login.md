# Function: *Login* #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.ReturnCodes.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Get.GetSingleInfotonByURI.md)  

----

## Description ##
The `_login` API call allows you to log into CM-Well and receive a corresponding authentication token. You pass the token to subsequent API calls as the value of the **X-CM-WELL-TOKEN** HTTP header. This enables the access permissions assigned to your user.

>**Note:** If you are running a privately compiled version of CM-Well, the default root user name and password are "root" and "root".

By default, tokens are valid for 24 hours after they are created. You can call `_login` again to receive a new token when the previous one expires. Optionally, you can request a custom expiration period (up to 60 days) when logging in.

If you browse to the CM-Well `_login` endpoint, you will be prompted to enter your username and password in a dialog. If you call `_login` programatically, you must pass your encoded credentials, using the **basic** or **digest** format, in the **Authorization** HTTP header. 

**Basic authentication credentials:**

Template: "Basic " + Base64(<username> + ':' + <password>)
Example:  "Basic " + Base64("mynewsapp" + ':' + "opensesame")


**Digest authentication credentials:**

The **digest** authentication method is a more complex (and more secure) multiple-step protocol (see [Digest Access Authentication](https://en.wikipedia.org/wiki/Digest_access_authentication) to learn more).

## Syntax ##

**URL:** <CMWellHost>/_login
**REST verb:** GET
**Mandatory parameters:** Authorization string (encoding username and password)

----------

**Template:**

    <CMWellHost>/_login

Or:

	<CMWellHost>/_login?exp=<expiration period>

**URL example:**
   `<cm-well-host>/_login`

**Curl example (REST API):**
    
For Basic authentication:

    curl -u myUser:myPassword <cm-well-host>/_login

For Digest authentication:

    curl -u myUser:myPassword --digest <cm-well-host>/_login


## Special Parameters ##

Parameter | Description | Values | Example | Reference
:----------|:-------------|:--------|:---------|:----------
exp | The time period before the token expires. Maximum allowed is 60 days. | Time string with days (d), hours (h) and minutes (m) integer values. Case-insensitive. Optional separators of any kind (spaces, dashes, etc.) | exp="30d", exp="3d 5h", exp="2h30m" | N/A

>**Note:** See **Description** section for details about constructing the **Authorization** HTTP header.

## Code Example ##

### Call ###

    curl -u myUser:myPassword <cm-well-host>/_login

### Results ###

Success:

    200 OK {"token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteVVzZXJuYW1lIiwiZXhwIjoxNDY2NTg5NzYzMDMzLCJyZXYiOjB9.17s3USkZaZYbifXsgmm8eaEmr66SP6udbuUQCLwcWKY"}

Failure (the user does not exist or the given password doesn't match the user):

    401 Unauthorized Not authenticated.

    401 - User/Pass don't match.


## Related Topics ##
[CM-Well Security Features](DevGuide.CM-WellSecurityFeatures.md)


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.ReturnCodes.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Get.GetSingleInfotonByURI.md)  

----