# CM-Well API Return Codes #

The following table describes common HTTP return codes you may receive in response to REST API calls.


Return Code | Description
:------------|:------------
200 | Operation succeeded
204 | No more results
404 | Infoton not found
401 | Unauthorized user, or password does not match user name
422 | The sub-graph you requested to delete does not exist
503 | Server is busy
5xx | Access problem